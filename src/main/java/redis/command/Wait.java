package redis.command;

import redis.RedisSocket;
import redis.config.RedisConfig;
import redis.exception.RedisException;
import redis.resp.RespBulkString;
import redis.resp.RespInteger;
import redis.resp.RespValue;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

import static redis.util.Logger.debug;

public final class Wait extends AbstractRedisCommand {
    private final int numberOfReplicas;
    private final int timeout;

    public Wait(List<RespValue> tokens, RedisConfig config) {
        super(config);
        if (tokens.size() < 3 || !(tokens.get(1) instanceof RespBulkString numberOfReplicasResp)
            || !(tokens.get(2) instanceof RespBulkString timeoutResp)) {
            throw new RedisException("WAIT command requires valid arguments");
        }

        this.numberOfReplicas = Integer.parseInt(numberOfReplicasResp.value());
        this.timeout = Integer.parseInt(timeoutResp.value());
    }

    @Override
    public void handle(RedisSocket client) {
        debug("Received WAIT command with numslaves: %d and timeout: %d", numberOfReplicas, numberOfReplicas);
        RespInteger response = null;
//        if (lastCommand instanceof Set) {
//            debug("provalilsya taki ovde: %s, %s", lastCommand, wait);
//            var numReplicas = Math.max(wait.numberOfReplicas(), replicationService.getReplicaNumber());
//            waitLatch = new CountDownLatch(numReplicas);
//            replicationService.propagate(new RespArray(List.of(
//                    new RespBulkString("REPLCONF"),
//                    new RespBulkString("GETACK"),
//                    new RespBulkString("*")
//            )), waitLatch);
//            try {
//                waitLatch.await(wait.timeout(), TimeUnit.MILLISECONDS);
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//                debug("WAIT command interrupted: %s", e.getMessage());
//                throw new RedisException(e);
//            }
//            if (waitLatch)
//            var replicasReplied = numReplicas - waitLatch.getCount();
//            debug("WAIT command completed, %d replicas replied", replicasReplied);
//            response = new RespInteger(replicasReplied);
        sendResponse(client, new RespInteger(0));
    }

    public int numberOfReplicas() {
        return numberOfReplicas;
    }

    public int timeout() {
        return timeout;
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
