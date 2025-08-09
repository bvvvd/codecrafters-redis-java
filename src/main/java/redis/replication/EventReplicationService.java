package redis.replication;

import redis.MainEventLoop;
import redis.RedisSocket;
import redis.config.RedisConfig;
import redis.resp.Parser;
import redis.resp.RespArray;
import redis.resp.RespBulkString;
import redis.resp.RespSimpleString;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;

import static redis.util.Logger.debug;
import static redis.util.Logger.error;

public class EventReplicationService {
    private final RedisConfig config;
    private final Parser parser;
    private final String fullResyncMessage;
    private final AtomicLong offset;
    private volatile boolean fullSyncCompleted;
    private final Set<SocketChannel> replicas;

    public EventReplicationService(RedisConfig redisConfig, Parser parser, long initialOffset) {
        config = redisConfig;
        this.parser = parser;
        this.fullResyncMessage = "+FULLRESYNC " + config.getReplicationId() + " 0\r\n";
        fullSyncCompleted = false;
        replicas = new CopyOnWriteArraySet<>();
        offset = new AtomicLong(initialOffset);
    }

    public SocketChannel establishReplication() throws IOException {
        if (config.getRole().equalsIgnoreCase("master")) {
            throw new IllegalStateException("Replication can only be established from a slave to a master.");
        }
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
        return socketChannel;
    }

    private boolean handshake(RedisSocket socket) {
        write(socket, new RespArray(List.of(new RespBulkString("PING"))));
        byte[] buffer = socket.read(256).orElse(null);
        if (buffer == null || !(parser.parse(buffer).getFirst() instanceof RespSimpleString respSimpleString) || !respSimpleString.value().equalsIgnoreCase("PONG")) {
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

    private void write(RedisSocket socket, RespArray respArray) {
        socket.write(respArray.serialize());
    }

    public void addReplica(SocketChannel channel) {
        if (config.getRole().equalsIgnoreCase("master")) {
            replicas.add(channel);
        }
    }

    public void propagate(RespArray array, Selector selector) throws ClosedChannelException {
        for (SocketChannel replica : replicas) {
            if (replica.isOpen() && replica.isConnected()) {
                SelectionKey key = replica.register(selector, SelectionKey.OP_WRITE);
                MainEventLoop.ClientState state = new MainEventLoop.ClientState(key);
                state.append(ByteBuffer.wrap(array.serialize()));
                key.attach(state);
            } else {
                replicas.remove(replica);
            }
        }
        offset.addAndGet(array.serialize().length);
    }

    public long getOffset() {
        return offset.get();
    }

    public void moveOffset(int length) {
        offset.addAndGet(length);
    }

    public int getReplicaNumber() {
        return replicas.size();
    }
}
