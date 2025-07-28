package redis.replication;

import redis.command.Set;
import redis.config.Constants;
import redis.config.RedisConfig;
import redis.exception.RedisException;
import redis.resp.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static redis.config.Constants.PONG;
import static redis.util.Logger.debug;
import static redis.util.Logger.error;

public class ReplicationService implements AutoCloseable {
    private final List<Socket> replicationConnections;
    private final ExecutorService propagationExecutor;
    private final AtomicLong offset;
    private final Parser parser;
    private volatile boolean fullSyncCompleted;

    public ReplicationService(ExecutorService propagationExecutor, long initialOffset) {
        this.replicationConnections = new CopyOnWriteArrayList<>();
        this.propagationExecutor = propagationExecutor;
        this.offset = new AtomicLong(initialOffset);
        this.parser = new Parser();
    }

    public void addReplica(Socket clientSocket) {
        replicationConnections.add(clientSocket);
    }

    public void propagate(Set command) {
        debug("propagating command to replica: " + command);
        offset.addAndGet(command.getOriginalBytes().length);
        debug("moved offset by: " + command.getOriginalBytes().length);

        replicationConnections.forEach(connection -> propagationExecutor.submit(() -> {
            if (connection.isClosed()) {
                replicationConnections.remove(connection);
            } else {
                OutputStream outputStream;
                try {
                    debug("propagating command to replica: " + command);
                    outputStream = connection.getOutputStream();
                    outputStream.write(command.getOriginalBytes());
                } catch (IOException e) {
                    error("Error while propagating command to replica: " + e.getMessage());
                    throw new RedisException(e);
                }
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
                if (connection.isClosed()) {
                    replicationConnections.remove(connection);
                } else {
                    OutputStream outputStream;
                    outputStream = connection.getOutputStream();
                    outputStream.write(serialize);
                    debug("propagated command to replica: " + respValue);
                }
            } catch (Exception e) {
                error("Error while propagating command to replica: " + e.getMessage());
                throw new RedisException(e);
            }
        }));
    }

    public void establish(RedisConfig config) throws IOException {
        InetSocketAddress inetSocketAddress = new InetSocketAddress(config.getMasterHost(), config.getMasterPort());
        SocketChannel socket = SocketChannel.open(inetSocketAddress);
        ByteBuffer buffer = ByteBuffer.allocate(256);
        while (!fullSyncCompleted) {
            ping(config, socket, buffer);
            replConf(config, socket, buffer);
            psync(config, socket);

            buffer.clear();
            socket.read(buffer);
            byte[] bytes = buffer.array();
            byte[] command = Arrays.copyOfRange(bytes, 1, 11);
            String presumablyFullResync = new String(command);
            debug("Received presumably full resync command from master: %s", presumablyFullResync);
            if (presumablyFullResync.equalsIgnoreCase("FULLRESYNC")) {
                handleFullsync(socket, buffer, config);
                fullSyncCompleted = true;
            }
        }
    }

    private void ping(RedisConfig config, SocketChannel socket, ByteBuffer buffer) throws IOException {
        write(socket, new RespArray(List.of(new RespBulkString(Constants.PING))));
        debug("Sent PING command to master at %s:%d", config.getMasterHost(), config.getMasterPort());

        RespValue parsedRespValue = read(socket, buffer).getFirst();
        if (!(parsedRespValue instanceof RespSimpleString respSimpleString) || !respSimpleString.value().equalsIgnoreCase(PONG)) {
            throw new RedisException("failed to ping master " + parsedRespValue);
        }

        debug("Received PONG successfully");
    }

    private void replConf(RedisConfig config, SocketChannel socket, ByteBuffer buffer) throws IOException {
        write(socket, new RespArray(List.of(new RespBulkString("REPLCONF"), new RespBulkString("listening-port"), new RespBulkString(Integer.toString(config.getPort())))));
        debug("Sent REPLCONF command to master at %s:%d", config.getMasterHost(), config.getMasterPort());

        RespValue parsedRespValue = read(socket, buffer).getFirst();
        if (parsedRespValue instanceof RespSimpleString respSimpleString && respSimpleString.value().equals("OK")) {
            debug("Replication configuration first stage set successfully with master at %s:%d", config.getMasterHost(), config.getMasterPort());
        } else {
            error("Failed to set replication configuration with master at %s:%d", config.getMasterHost(), config.getMasterPort());
            throw new RedisException("Failed to set replication configuration with master");
        }

        write(socket, new RespArray(List.of(new RespBulkString("REPLCONF"), new RespBulkString("capa"), new RespBulkString("psync2"))));

        parsedRespValue = read(socket, buffer).getFirst();
        if (parsedRespValue instanceof RespSimpleString replConfSecondResult && replConfSecondResult.value().equals("OK")) {
            debug("Replication configuration set successfully with master at %s:%d", config.getMasterHost(), config.getMasterPort());
        } else {
            error("Failed to set replication configuration with master at %s:%d", config.getMasterHost(), config.getMasterPort());
            throw new RedisException("Failed to set replication configuration with master");
        }
    }

    private void psync(RedisConfig config, SocketChannel socket) throws IOException {
        write(socket, new RespArray(List.of(new RespBulkString("PSYNC"), new RespBulkString("?"), new RespBulkString("-1"))));
        debug("Sent PSYNC command to master at %s:%d", config.getMasterHost(), config.getMasterPort());
    }

    private void handleFullsync(SocketChannel socket, ByteBuffer buffer, RedisConfig config) throws IOException {
        debug("Full resync initiated with master");
        int index = 0;
        byte[] bytes = buffer.array();
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

            createDumpFile(bytes, index + 2, length, config);

//            List<RespValue> replicationMessages = handlePropagatedMessages(buffer, index + 2 + length);
//            if (!replicationMessages.isEmpty()) {
//                return replicationMessages;
//            }
        }

//        buffer.clear()
//        Arrays.fill(b, (byte) 0);
//        inputStream.read(buffer);
//        index = 0;
//        if (buffer[index] == '$') {
//            debug("got rdb with the next read");
//            index++;
//            int length = 0;
//            while (buffer[index] != '\r') {
//                length = length * 10 + buffer[index] - '0';
//                index++;
//            }
//
//            createDumpFile(buffer, index + 2, length);
//
//            List<RespValue> replicationMessages = handlePropagatedMessages(buffer, index + 2 + length);
//            if (!replicationMessages.isEmpty()) {
//                return replicationMessages;
//            }
//            Arrays.fill(buffer, (byte) 0);
//            inputStream.read(buffer);
//            index = 0;
//        }
//
//        return handlePropagatedMessages(buffer, index);
    }

    private void createDumpFile(byte[] buffer, int from, int length, RedisConfig config) throws IOException {
        File file = new File("%s/%s".formatted(config.getDir(), config.getDbFileName()));
        File parentFile = file.getParentFile();
        parentFile.mkdirs();
        try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            fileOutputStream.write(buffer, from, length);
        }
        debug("created a dump file %s: %s", file, new String(Arrays.copyOfRange(buffer, from, from + length)));
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

    private void write(SocketChannel socket, RespArray respArray) throws IOException {
        socket.write(ByteBuffer.wrap(respArray.serialize()));
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

    private void clear(byte[] buffer) {
        Arrays.fill(buffer, (byte) 0);
    }
}
