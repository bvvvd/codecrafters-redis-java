package redis.replication;

import redis.command.Set;
import redis.exception.RedisException;
import redis.resp.RespValue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static redis.util.Logger.debug;
import static redis.util.Logger.error;

public class ReplicationService implements AutoCloseable {
    private final List<Socket> replicationConnections;
    private final ExecutorService propagationExecutor;
    private final AtomicLong offset;

    public ReplicationService(ExecutorService propagationExecutor, long initialOffset) {
        this.replicationConnections = new CopyOnWriteArrayList<>();
        this.propagationExecutor = propagationExecutor;
        offset = new AtomicLong(initialOffset);
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
}
