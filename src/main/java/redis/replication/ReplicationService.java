package redis.replication;

import redis.RedisCommandBuilder;
import redis.RedisSocket;
import redis.command.RedisCommand;
import redis.command.Set;
import redis.config.Constants;
import redis.config.RedisConfig;
import redis.exception.RedisException;
import redis.resp.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static redis.config.Constants.PONG;
import static redis.util.Logger.debug;
import static redis.util.Logger.error;

public class ReplicationService implements AutoCloseable {
    private final RedisConfig config;
    private final List<RedisSocket> replicationConnections;
    private final ExecutorService propagationExecutor;
    private final AtomicLong offset;
    private final Parser parser;
    private final RedisCommandBuilder commandBuilder;
    private volatile boolean fullSyncCompleted;

    public ReplicationService(ExecutorService propagationExecutor, long initialOffset, RedisConfig config) {
        this.config = config;
        this.replicationConnections = new CopyOnWriteArrayList<>();
        this.propagationExecutor = propagationExecutor;
        this.offset = new AtomicLong(initialOffset);
        this.parser = new Parser();
        this.commandBuilder = new RedisCommandBuilder(config, new ConcurrentHashMap<>(), this);
    }

    public void addReplica(RedisSocket clientSocket) {
        replicationConnections.add(clientSocket);
    }

    public void propagate(Set command) {
        debug("propagating command to replica: " + command);
        offset.addAndGet(command.getOriginalBytes().length);
        debug("moved offset by: " + command.getOriginalBytes().length);

        replicationConnections.forEach(connection -> propagationExecutor.submit(() -> {
            if (!connection.isConnected()) {
                replicationConnections.remove(connection);
            } else {
                debug("propagating command to replica: " + command);
                connection.write(command.getOriginalBytes());
            }
        }));
    }

    public void propagate(RespValue respValue, CountDownLatch waitLatch) {
        debug("propagating command to replica: " + respValue);
        byte[] serialize = respValue.serialize();
        debug("moved offset by: " + serialize.length);
        offset.addAndGet(serialize.length);
        replicationConnections.forEach(connection -> propagationExecutor.submit(() -> {
            try {
                debug("getacking replica: " + connection);
                if (!connection.isConnected()) {
                    replicationConnections.remove(connection);
                } else {
                    connection.write(serialize);
                    debug("propagated command to replica: " + respValue);
                }
            } catch (Exception e) {
                error("Error while propagating command to replica: " + e.getMessage());
                throw new RedisException(e);
            }
        }));
    }

    public void establish() throws IOException {
        InetSocketAddress inetSocketAddress = new InetSocketAddress(config.getMasterHost(), config.getMasterPort());
        RedisSocket socket = new RedisSocket(SocketChannel.open(inetSocketAddress));
        try {
            while (!fullSyncCompleted) {
                ping(socket);
                replConf(socket);
                psync(socket);
                byte[] input = socket.read(256).get();
                RespValue first = parser.parse(input).getFirst();
                if (first instanceof RespArray presumablyFullResync
                    && presumablyFullResync.values().getFirst() instanceof RespBulkString commandToken
                    && commandToken.value().equalsIgnoreCase("FULLRESYNC")) {
                    debug("Received presumably full resync command from master: %s", presumablyFullResync);
                    List<RedisCommand> commands = handleFullsync(socket, input);
                    commands.forEach(propagatedCommand -> propagatedCommand.handle(socket));
                    fullSyncCompleted = true;
                }
            }
        } catch (Exception e) {
            error("Failed to establish replication with master %s%n", e.getMessage());
        }

        propagationExecutor.submit(() -> {
            while (socket.isConnected()) {
                try {
                    debug("Waiting for commands from master at %s:%d", config.getMasterHost(), config.getMasterPort());
                    List<RespValue> readValues = parser.parse(socket.read(256).get());
                    List<RedisCommand> commands = commandBuilder.build(readValues);
                    commands.forEach(command -> command.handle(socket));
                } catch (Exception e) {
                    error("Error serving client: %s%n", e.getMessage());
                    throw new RedisException(e);
                }
            }
        });
    }

    private void ping(RedisSocket socket) {
        write(socket, new RespArray(List.of(new RespBulkString(Constants.PING))));
        debug("Sent PING command to master at %s:%d", config.getMasterHost(), config.getMasterPort());

        RespValue parsedRespValue = parser.parse(socket.read(256).get()).getFirst();
        if (!(parsedRespValue instanceof RespSimpleString respSimpleString) || !respSimpleString.value().equalsIgnoreCase(PONG)) {
            throw new RedisException("failed to ping master " + parsedRespValue);
        }

        debug("Received PONG successfully");
    }

    private void replConf(RedisSocket socket) throws IOException {
        write(socket, new RespArray(List.of(new RespBulkString("REPLCONF"), new RespBulkString("listening-port"), new RespBulkString(Integer.toString(config.getPort())))));
        debug("Sent REPLCONF command to master at %s:%d", config.getMasterHost(), config.getMasterPort());

        RespValue parsedRespValue = parser.parse(socket.read(256).get()).getFirst();
        if (parsedRespValue instanceof RespSimpleString respSimpleString && respSimpleString.value().equals("OK")) {
            debug("Replication configuration first stage set successfully with master at %s:%d", config.getMasterHost(), config.getMasterPort());
        } else {
            error("Failed to set replication configuration with master at %s:%d", config.getMasterHost(), config.getMasterPort());
            throw new RedisException("Failed to set replication configuration with master");
        }

        write(socket, new RespArray(List.of(new RespBulkString("REPLCONF"), new RespBulkString("capa"), new RespBulkString("psync2"))));

        parsedRespValue = parser.parse(socket.read(256).get()).getFirst();
        if (parsedRespValue instanceof RespSimpleString replConfSecondResult && replConfSecondResult.value().equals("OK")) {
            debug("Replication configuration set successfully with master at %s:%d", config.getMasterHost(), config.getMasterPort());
        } else {
            error("Failed to set replication configuration with master at %s:%d", config.getMasterHost(), config.getMasterPort());
            throw new RedisException("Failed to set replication configuration with master");
        }
    }

    private void psync(RedisSocket socket) {
        write(socket, new RespArray(List.of(new RespBulkString("PSYNC"), new RespBulkString("?"), new RespBulkString("-1"))));
        debug("Sent PSYNC command to master at %s:%d", config.getMasterHost(), config.getMasterPort());
    }

    private List<RedisCommand> handleFullsync(RedisSocket socket, byte[] bytes) throws IOException {
        debug("Full resync initiated with master");
        int index = 0;
        while (bytes[index] != '\r') {
            index++;
        }
        index += 2;

        if (bytes[index] == '$') {
            debug("got rdb along with the fullresync command");
            index++;
            int length = 0;
            while (bytes[index] != '\r') {
                length = length * 10 + bytes[index] - '0';
                index++;
            }

            createDumpFile(bytes, index + 2, length);

            List<RespValue> replicationMessages = handlePropagatedMessages(bytes, index + 2 + length);
            if (!replicationMessages.isEmpty()) {
                return commandBuilder.build(replicationMessages);
            }
        }

        bytes = socket.read(256).get();
        index = 0;
        if (bytes[index] == '$') {
            debug("got rdb with the next read");
            index++;
            int length = 0;
            while (bytes[index] != '\r') {
                length = length * 10 + bytes[index] - '0';
                index++;
            }

            createDumpFile(bytes, index + 2, length);

            List<RespValue> replicationMessages = handlePropagatedMessages(bytes, index + 2 + length);
            if (!replicationMessages.isEmpty()) {
                return commandBuilder.build(replicationMessages);
            }
            bytes = socket.read(256).get();
            index = 0;
        }

        List<RespValue> readValues = handlePropagatedMessages(bytes, index);
        if (!readValues.isEmpty()) {
            return commandBuilder.build(readValues);
        }

        return Collections.emptyList();
    }

    private void createDumpFile(byte[] buffer, int from, int length) throws IOException {
        File file = new File("%s/%s".formatted(config.getDir(), config.getDbFileName()));
        File parentFile = file.getParentFile();
        parentFile.mkdirs();
        try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            fileOutputStream.write(buffer, from, length);
        }
        debug("created a dump file %s: %s", file, new String(Arrays.copyOfRange(buffer, from, from + length)));
    }

    private List<RespValue> handlePropagatedMessages(byte[] buffer, int index) {
        try {
            debug("copying propagated messages from index %d %s", index, new String(Arrays.copyOfRange(buffer, index, index + 5)));
            return parser.parse(Arrays.copyOfRange(buffer, index, buffer.length));
        } catch (Exception e) {
            error("skipping invalid data %s", e.getMessage());
//            throw new RedisException("Failed to parse propagated messages from master: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    @Override
    public void close() {
        propagationExecutor.shutdown();
    }

    public int getReplicaNumber() {
        return replicationConnections.size();
    }

    public void moveOffset(int length) {
        offset.addAndGet(length);
    }

    public long getOffset() {
        return offset.get();
    }

    private void write(RedisSocket socket, RespArray respArray) {
        socket.write(respArray.serialize());
    }

    private List<RespValue> read(SocketChannel socket, ByteBuffer buffer) throws IOException {
        buffer.clear();
        socket.read(buffer);
        try {
            return parser.parse(buffer.array());
        } catch (Exception e) {
            error("Failed to parse response from master: %s %n", e.getMessage());
            throw new RedisException("Failed to parse response from master" + e);
        }
    }
}
