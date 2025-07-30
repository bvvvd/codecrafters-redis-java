package redis.command;

import redis.RedisSocket;
import redis.cache.CachedValue;
import redis.config.RedisConfig;
import redis.replication.ReplicationService;
import redis.resp.RespArray;
import redis.resp.RespBulkString;
import redis.resp.RespInteger;
import redis.resp.RespValue;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

public final class RPush extends AbstractRedisCommand {
    public static final String CODE = "RPUSH";
    private final RespBulkString key;
    private final RespBulkString value;
    private final byte[] originalBytes;
    private final ConcurrentMap<RespValue, CachedValue<RespValue>> cache;

    public RPush(List<RespValue> tokens, byte[] originalBytes, RedisConfig config, ConcurrentMap<RespValue, CachedValue<RespValue>> cache, ReplicationService replicationService) {
        super(config, replicationService);

        if (tokens.size() < 3 || !(tokens.get(1) instanceof RespBulkString key) || !(tokens.get(2) instanceof RespBulkString value)) {
            throw new IllegalArgumentException("RPush command requires at least a key and a value");
        }
        this.key = key;
        this.value = value;
        this.originalBytes = originalBytes;
        this.cache = cache;
    }

    @Override
    protected void handleCommand(RedisSocket client) {
        cache.put(key, new CachedValue<>(new RespArray(List.of(value)), -1));
        sendResponse(client, new RespInteger(1));
    }

    @Override
    public String toString() {
        return "RPush{" +
               "key=" + key +
               ", value=" + value +
               ", originalBytes=" + Arrays.toString(originalBytes) +
               ", cache=" + cache +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        RPush rPush = (RPush) o;
        return Objects.equals(key, rPush.key) && Objects.equals(value, rPush.value) && Objects.deepEquals(originalBytes, rPush.originalBytes) && Objects.equals(cache, rPush.cache);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, Arrays.hashCode(originalBytes), cache);
    }
}
