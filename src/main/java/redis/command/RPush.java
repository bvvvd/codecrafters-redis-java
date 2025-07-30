package redis.command;

import redis.RedisSocket;
import redis.cache.CachedValue;
import redis.config.RedisConfig;
import redis.replication.ReplicationService;
import redis.resp.RespArray;
import redis.resp.RespBulkString;
import redis.resp.RespInteger;
import redis.resp.RespValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

public final class RPush extends AbstractRedisCommand {
    public static final String CODE = "RPUSH";
    private final RespBulkString key;
    private final List<RespValue> values;
    private final byte[] originalBytes;
    private final ConcurrentMap<RespValue, CachedValue<RespValue>> cache;

    public RPush(List<RespValue> tokens, byte[] originalBytes, RedisConfig config, ConcurrentMap<RespValue, CachedValue<RespValue>> cache, ReplicationService replicationService) {
        super(config, replicationService);

        if (tokens.size() < 3 || !(tokens.get(1) instanceof RespBulkString key)) {
            throw new IllegalArgumentException("RPush command requires at least a key and a value");
        }
        List<RespValue> respValues = new ArrayList<>();
        for (int i = 2; i < tokens.size(); i++) {
            if (tokens.get(i) instanceof RespBulkString value) {
                respValues.add(value);
            } else {
                throw new IllegalArgumentException("RPush command requires values to be of type RespBulkString");
            }
        }
        this.key = key;
        this.values = respValues;
        this.originalBytes = originalBytes;
        this.cache = cache;
    }

    @Override
    protected void handleCommand(RedisSocket client) {
        CachedValue<RespValue> cachedValue = cache.get(key);
        if (cachedValue == null) {
            cache.put(key, new CachedValue<>(new RespArray(new ArrayList<>(values)), -1));
            sendResponse(client, new RespInteger(values.size()));
        } else {
            RespArray array = (RespArray) cachedValue.getValue();
            array.values().addAll(values);
            sendResponse(client, new RespInteger(array.values().size()));
        }
    }

    @Override
    public String toString() {
        return "RPush{" +
               "key=" + key +
               ", value=" + values +
               ", originalBytes=" + Arrays.toString(originalBytes) +
               ", cache=" + cache +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        RPush rPush = (RPush) o;
        return Objects.equals(key, rPush.key) && Objects.equals(values, rPush.values) && Objects.deepEquals(originalBytes, rPush.originalBytes) && Objects.equals(cache, rPush.cache);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, values, Arrays.hashCode(originalBytes), cache);
    }
}
