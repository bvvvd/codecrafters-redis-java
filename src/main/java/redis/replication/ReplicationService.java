package redis.replication;

import redis.RedisCommandBuilder;
import redis.RedisSocket;
import redis.cache.CachedValue;
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
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
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
    private final String fullResyncMessage;
    private volatile RedisCommand lastCommand;
    private volatile CountDownLatch waitLatch;

    public ReplicationService(ExecutorService propagationExecutor, long initialOffset, RedisConfig config, ConcurrentMap<RespValue, CachedValue<RespValue>> cache) {
        this.config = config;
        this.replicationConnections = new CopyOnWriteArrayList<>();
        this.propagationExecutor = propagationExecutor;
        this.offset = new AtomicLong(initialOffset);
        this.parser = new Parser();
        this.commandBuilder = new RedisCommandBuilder(config, cache, this);
        this.fullResyncMessage = "+FULLRESYNC " + config.getReplicationId() + " 0\r\n";
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

    public void propagate(RespValue respValue) {
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
        SocketChannel socketChannel = SocketChannel.open(inetSocketAddress);
        socketChannel.configureBlocking(true);
        RedisSocket socket = new RedisSocket(socketChannel);
        try {
            while (!fullSyncCompleted) {
                if (handshake(socket)) {
                    byte[] rdbSizeHeader = socket.readUntil((byte) '\n');
                    int rdbSize = Integer.parseInt(new String(Arrays.copyOfRange(rdbSizeHeader, 1, rdbSizeHeader.length - 2)));
                    Optional<byte[]> read = socket.read(rdbSize);
                    if (read.isPresent()) {
                        createDumpFile(read.get());
                        fullSyncCompleted = true;
                    }
                }
            }
        } catch (Exception e) {
            error("Failed to establish replication with master %s%n", e.getMessage());
        }

        propagationExecutor.submit(() -> {
            while (socket.isConnected()) {
                try {
                    debug("Waiting for commands from master at %s:%d", config.getMasterHost(), config.getMasterPort());
                    socket.read(256).ifPresent(read -> {
                        List<RespValue> respValues = parser.parse(read);
                        List<RedisCommand> commands = commandBuilder.build(respValues);
                        commands.forEach(command -> command.handle(socket));
                    });
                } catch (Exception e) {
                    error("Error serving client: %s%n", e.getMessage());
                    throw new RedisException(e);
                }
            }
        });
    }

    private boolean handshake(RedisSocket socket) {
        write(socket, new RespArray(List.of(new RespBulkString(Constants.PING))));
        byte[] buffer = socket.read(256).orElse(null);
        if (buffer == null || !(parser.parse(buffer).getFirst() instanceof RespSimpleString respSimpleString) || !respSimpleString.value().equalsIgnoreCase(PONG)) {
            return false;
        }

        write(socket, new RespArray(List.of(new RespBulkString("REPLCONF"), new RespBulkString("listening-port"), new RespBulkString(Integer.toString(config.getPort())))));
        buffer = socket.read(256).orElse(null);
        if (buffer == null || !(parser.parse(buffer).getFirst() instanceof RespSimpleString respBulkString) || !respBulkString.value().equals("OK")) {
            error("Failed to set replication configuration with master at %s:%d", config.getMasterHost(), config.getMasterPort());
            return false;
        }

        write(socket, new RespArray(List.of(new RespBulkString("REPLCONF"), new RespBulkString("capa"), new RespBulkString("psync2"))));

        buffer = socket.read(256).orElse(null);
        if (buffer == null || !(parser.parse(buffer).getFirst() instanceof RespSimpleString replConfSecondResult) || !replConfSecondResult.value().equals("OK")) {
            error("Failed to set replication configuration with master at %s:%d", config.getMasterHost(), config.getMasterPort());
            return false;
        }

        write(socket, new RespArray(List.of(new RespBulkString("PSYNC"), new RespBulkString("?"), new RespBulkString("-1"))));

        buffer = socket.read((fullResyncMessage.getBytes(StandardCharsets.UTF_8).length)).orElse(null);
        return buffer != null;
    }

    private void createDumpFile(byte[] buffer) throws IOException {
        File file = new File("%s/%s".formatted(config.getDir(), config.getDbFileName()));
        File parentFile = file.getParentFile();
        parentFile.mkdirs();
        try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            fileOutputStream.write(buffer);
        }
        debug("created a dump file %s: %s", file, new String(buffer));
    }

    @Override
    public void close() {
        propagationExecutor.shutdown();
    }

    public int getReplicaNumber() {
        return replicationConnections.size();
    }

    public void moveOffset(int length) {
        debug("Moving offset by: %d", length);
        offset.addAndGet(length);
    }

    public long getOffset() {
        return offset.get();
    }

    private void write(RedisSocket socket, RespArray respArray) {
        socket.write(respArray.serialize());
    }

    public void setLastCommand(RedisCommand redisCommand) {
        this.lastCommand = redisCommand;
    }

    public RedisCommand getLastCommand() {
        return lastCommand;
    }

    public void setLatch(CountDownLatch latch) {
        this.waitLatch = latch;
    }

    public CountDownLatch getWaitLatch() {
        return waitLatch;
    }
}
