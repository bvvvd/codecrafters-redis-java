package redis.command;

import redis.RedisSocket;
import redis.config.RedisConfig;
import redis.exception.RedisException;
import redis.persistence.DumpFileReader;
import redis.resp.RespArray;
import redis.resp.RespBulkString;
import redis.resp.RespValue;

import java.util.ArrayList;
import java.util.List;

import static redis.util.Logger.debug;

public final class Keys extends AbstractRedisCommand {
    private final DumpFileReader dumpFileReader;

    public Keys(List<RespValue> tokens, RedisConfig config) {
        super(config);
        if (tokens.size() < 2 || !(tokens.get(1) instanceof RespBulkString pattern)
            || pattern.value().isBlank()) {
            throw new RedisException("KEYS command requires a valid pattern argument");
        }

        this.dumpFileReader = new DumpFileReader(config);
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || obj != null && obj.getClass() == this.getClass();
    }

    @Override
    public int hashCode() {
        return 1;
    }

    @Override
    public String toString() {
        return "Keys[]";
    }

    @Override
    public void handle(RedisSocket client) {
        debug("Received KEYS command");
        List<RespValue> values = new ArrayList<>();
        dumpFileReader.read().keys().forEach(
                key -> values.add(new RespBulkString(key))
        );
        sendResponse(client, new RespArray(values));
    }
}
