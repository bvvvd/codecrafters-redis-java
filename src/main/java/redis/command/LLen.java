package redis.command;

import redis.RedisSocket;
import redis.cache.CachedValue;
import redis.config.RedisConfig;
import redis.replication.ReplicationService;
import redis.resp.RespArray;
import redis.resp.RespInteger;
import redis.resp.RespValue;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

public final class LLen extends AbstractRedisCommand {
    public static final String CODE = "LLEN";
    private final RespValue key;
    private final ConcurrentMap<RespValue, CachedValue<RespValue>> cache;

    public LLen(List<RespValue> tokens, ConcurrentMap<RespValue, CachedValue<RespValue>> cache, RedisConfig config, ReplicationService replicationService) {
        super(config, replicationService);

        if (tokens.size() != 2 || !(tokens.get(1) instanceof RespValue respKey)) {
            throw new IllegalArgumentException("LLEN command requires exactly one key argument");
        }

        this.key = respKey;
        this.cache = cache;
    }

    @Override
    protected void handleCommand(RedisSocket client) {
        sendResponse(client,
                new RespInteger(((RespArray) cache.getOrDefault(key,
                                new CachedValue<>(new RespArray(List.of()), -1))
                        .value()).values().size()));
    }

    @Override
    public String toString() {
        return "LLen{" +
               "key=" + key +
               ", cache=" + cache +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        LLen lLen = (LLen) o;
        return Objects.equals(key, lLen.key) && Objects.equals(cache, lLen.cache);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, cache);
    }
}
