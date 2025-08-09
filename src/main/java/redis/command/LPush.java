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
import redis.util.FirstThenAllLatch;
import redis.util.LockAndCondition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static redis.util.Logger.error;

public final class LPush extends AbstractRedisCommand {
    public static final String CODE = "LPUSH";
    private final RespBulkString key;
    private final List<RespValue> values;
    private final byte[] originalBytes;
    private final ConcurrentMap<RespValue, CachedValue<RespValue>> cache;

    public LPush(List<RespValue> tokens, byte[] originalBytes, RedisConfig config, ConcurrentMap<RespValue, CachedValue<RespValue>> cache, ReplicationService replicationService) {
        super(config, replicationService);

        if (tokens.size() < 3 || !(tokens.get(1) instanceof RespBulkString respKey)) {
            throw new RedisException("LPush command requires at least a key and a value");
        }
        List<RespValue> respValues = new ArrayList<>();
        for (int i = 2; i < tokens.size(); i++) {
            if (tokens.get(i) instanceof RespBulkString value) {
                respValues.add(value);
            } else {
                throw new RedisException("LPush command requires values to be of type RespBulkString");
            }
        }
        this.key = respKey;
        this.values = respValues.reversed();
        this.originalBytes = originalBytes;
        this.cache = cache;
    }

    @Override
    protected void handleCommand(RedisSocket client) {
        FirstThenAllLatch latch = replicationService.getPopLatch(key);
        try {
            CachedValue<RespValue> cachedValue = cache.get(key);
            List<RespValue> newArray = new CopyOnWriteArrayList<>(values);
            if (cachedValue != null && cachedValue.getValue() instanceof RespArray array) {
                newArray.addAll(array.values());
            }
            cache.put(key, new CachedValue<>(new RespArray(newArray), -1));
            latch.release();
            sendResponse(client, new RespInteger(newArray.size()));
        } catch (Exception e) {
            throw new RedisException("Error processing LPush command: " + e);
        }
    }

    @Override
    public String toString() {
        return "LPush{" +
               "key=" + key +
               ", values=" + values +
               ", originalBytes=" + Arrays.toString(originalBytes) +
               ", cache=" + cache +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        LPush lPush = (LPush) o;
        return Objects.equals(key, lPush.key) && Objects.equals(values, lPush.values) && Objects.deepEquals(originalBytes, lPush.originalBytes) && Objects.equals(cache, lPush.cache);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, values, Arrays.hashCode(originalBytes), cache);
    }
}
