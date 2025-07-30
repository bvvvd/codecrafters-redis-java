package redis.command;

import redis.RedisSocket;
import redis.cache.CachedValue;
import redis.config.RedisConfig;
import redis.replication.ReplicationService;
import redis.resp.RespArray;
import redis.resp.RespBulkString;
import redis.resp.RespValue;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

public final class LPop extends AbstractRedisCommand {
    public static final String CODE = "LPOP";
    private final RespBulkString key;
    private final ConcurrentMap<RespValue, CachedValue<RespValue>> cache;
    private final byte[] originalBytes;

    public LPop(List<RespValue> tokens, byte[] originalBytes, ConcurrentMap<RespValue, CachedValue<RespValue>> cache, RedisConfig config, ReplicationService replicationService) {
        super(config, replicationService);

        if (tokens.size() < 2 || !(tokens.get(1) instanceof RespBulkString respKey)) {
            throw new IllegalArgumentException("LPop command requires at least one key argument");
        }

        this.key = respKey;
        this.cache = cache;
        this.originalBytes = originalBytes;
    }

    @Override
    protected void handleCommand(RedisSocket client) {
        List<RespValue> cachedValues
                = ((RespArray) cache.getOrDefault(key,
                        new CachedValue<>(
                                new RespArray(List.of(
                                        new RespBulkString(null))), -1))
                .getValue())
                .values();
        sendResponse(client, cachedValues.removeFirst());
    }

    @Override
    public String toString() {
        return "LPop{" +
               "key=" + key +
               ", cache=" + cache +
               ", originalBytes=" + Arrays.toString(originalBytes) +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        LPop lPop = (LPop) o;
        return Objects.equals(key, lPop.key) && Objects.equals(cache, lPop.cache) && Objects.deepEquals(originalBytes, lPop.originalBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, cache, Arrays.hashCode(originalBytes));
    }
}
