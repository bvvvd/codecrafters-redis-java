package redis.command;

import redis.RedisSocket;
import redis.cache.CachedValue;
import redis.config.RedisConfig;
import redis.exception.RedisException;
import redis.replication.ReplicationService;
import redis.resp.RespArray;
import redis.resp.RespBulkString;
import redis.resp.RespInteger;
import redis.resp.RespValue;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

public final class LRange extends AbstractRedisCommand {
    public static final String CODE = "LRANGE";
    private final RespValue key;
    private final long start;
    private final long end;
    private final ConcurrentMap<RespValue, CachedValue<RespValue>> cache;

    public LRange(List<RespValue> tokens, ConcurrentMap<RespValue, CachedValue<RespValue>> cache, RedisConfig config, ReplicationService replicationService) {
        super(config, replicationService);

        if (tokens.size() < 4 || !(tokens.get(1) instanceof RespBulkString replKey)
            || !(tokens.get(2) instanceof RespBulkString replStart)
            || !(tokens.get(3) instanceof RespBulkString replEnd)) {
            throw new RedisException("Invalid LRANGE command format. Expected: LRANGE key start end");
        }

        this.key = replKey;
        this.start = Integer.parseInt(replStart.value());
        this.end = Integer.parseInt(replEnd.value()) + 1;
        this.cache = cache;
    }

    @Override
    protected void handleCommand(RedisSocket client) {
        CachedValue<RespValue> cachedValue = cache.get(key);
        if (cachedValue == null || !(cachedValue.getValue() instanceof RespArray array)) {
            sendResponse(client, new RespArray(List.of()));
        } else {
            List<RespValue> values = array.values().subList((int) start, Math.min(array.values().size(), (int) end));
            sendResponse(client, new RespArray(values));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        LRange lRange = (LRange) o;
        return start == lRange.start && end == lRange.end && Objects.equals(key, lRange.key) && Objects.equals(cache, lRange.cache);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, start, end, cache);
    }

    @Override
    public String toString() {
        return "LRange{" +
               "key=" + key +
               ", start=" + start +
               ", end=" + end +
               ", cache=" + cache +
               '}';
    }
}
