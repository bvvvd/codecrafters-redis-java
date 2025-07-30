package redis.command;

import redis.RedisSocket;
import redis.config.RedisConfig;
import redis.exception.RedisException;
import redis.replication.ReplicationService;
import redis.resp.RespArray;
import redis.resp.RespBulkString;
import redis.resp.RespInteger;
import redis.resp.RespValue;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static redis.util.Logger.debug;

public final class Wait extends AbstractRedisCommand {
    public static final String CODE = "WAIT";
    private final int numberOfReplicas;
    private final int timeout;

    public Wait(List<RespValue> tokens, RedisConfig config, ReplicationService replicationService) {
        super(config, replicationService);
        if (tokens.size() < 3 || !(tokens.get(1) instanceof RespBulkString numberOfReplicasResp)
            || !(tokens.get(2) instanceof RespBulkString timeoutResp)) {
            throw new RedisException("WAIT command requires valid arguments");
        }

        this.numberOfReplicas = Integer.parseInt(numberOfReplicasResp.value());
        this.timeout = Integer.parseInt(timeoutResp.value());
    }

    @Override
    public void handleCommand(RedisSocket client) {
        debug("Received WAIT command with numslaves: %d and timeout: %d", numberOfReplicas, numberOfReplicas);
        RespInteger response;
        if (replicationService.getLastCommand() instanceof Set) {
            var numReplicas = Math.max(numberOfReplicas, replicationService.getReplicaNumber());
            replicationService.setLatch(new CountDownLatch(numReplicas));
            replicationService.propagate(new RespArray(List.of(
                    new RespBulkString("REPLCONF"),
                    new RespBulkString("GETACK"),
                    new RespBulkString("*")
            )));
            try {
                replicationService.getWaitLatch().await(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                debug("WAIT command interrupted: %s", e.getMessage());
                throw new RedisException(e);
            }
            var replicasReplied = numReplicas - replicationService.getWaitLatch().getCount();
            debug("WAIT command completed, %d replicas replied", replicasReplied);
            response = new RespInteger(replicasReplied);
        } else {
            response = new RespInteger(replicationService.getReplicaNumber());
        }
        sendResponse(client, response);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (Wait) obj;
        return this.numberOfReplicas == that.numberOfReplicas &&
               this.timeout == that.timeout;
    }

    @Override
    public int hashCode() {
        return Objects.hash(numberOfReplicas, timeout);
    }

    @Override
    public String toString() {
        return "Wait[" +
               "numberOfReplicas=" + numberOfReplicas + ", " +
               "timeout=" + timeout + ']';
    }
}
