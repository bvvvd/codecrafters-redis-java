package redis.command;


import redis.RedisSocket;
import redis.config.RedisConfig;
import redis.exception.RedisException;
import redis.replication.ReplicationService;
import redis.resp.RespBulkString;
import redis.resp.RespValue;

import java.util.List;
import java.util.Objects;

import static redis.util.Logger.debug;

public final class Echo extends AbstractRedisCommand {
    private final RespValue value;

    public Echo(List<RespValue> tokens, RedisConfig config, ReplicationService replicationService) {
        super(config, replicationService);
        if (tokens.size() < 2 || !(tokens.get(1) instanceof RespBulkString argument)) {
            throw new RedisException("ECHO command requires a valid message argument");
        }
        this.value = argument;
    }

    @Override
    public void handleCommand(RedisSocket client) {
        debug("Sending echo response: %s", value);
        sendResponse(client, value);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (Echo) obj;
        return Objects.equals(this.value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return "Echo[" +
               "value=" + value + ']';
    }
}
