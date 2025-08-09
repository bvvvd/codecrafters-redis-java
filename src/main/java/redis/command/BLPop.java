package redis.command;

import redis.RedisSocket;
import redis.cache.CachedValue;
import redis.config.RedisConfig;
import redis.exception.RedisException;
import redis.replication.ReplicationService;
import redis.resp.RespArray;
import redis.resp.RespBulkString;
import redis.resp.RespValue;
import redis.util.FirstThenAllLatch;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

import static redis.util.Logger.error;

public final class BLPop extends AbstractRedisCommand {
    public static final String CODE = "BLPOP";
    private final RespBulkString key;
    private final ConcurrentMap<RespValue, CachedValue<RespValue>> cache;

    public BLPop(List<RespValue> tokens, ConcurrentMap<RespValue, CachedValue<RespValue>> cache, RedisConfig config, ReplicationService replicationService) {
        super(config, replicationService);

        if (tokens.size() < 2 || !(tokens.get(1) instanceof RespBulkString respKey)) {
            throw new RedisException("BLPOP command requires a key argument");
        }

        this.key = respKey;
        this.cache = cache;
    }

    @Override
    protected void handleCommand(RedisSocket client) {
        FirstThenAllLatch lock = replicationService.getPopLatch(key);

        try {
            Executors.newSingleThreadExecutor().submit(() -> {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } finally {
                    lock.release();
                }
            });
            lock.await(() -> cache.computeIfPresent(key, (k, cachedValue) -> {
                RespArray array = ((RespArray) cachedValue.value());
                if (array.values().size() == 1) {
                    RespArray response = new RespArray(List.of(key, array.values().getFirst()));
                    sendResponse(client, response);
                    return null;
                } else {
                    RespArray response = new RespArray(List.of(key, array.values().removeFirst()));
                    sendResponse(client, response);
                    return cachedValue;
                }
            }));
        } catch (Exception e) {
            Thread.currentThread().interrupt();
            error("Error handling BLPop command: " + e.getMessage());
            throw new RedisException("Error handling BLPop command: " + e);
        }
    }

    @Override
    public String toString() {
        return "BLPop{" +
               "key=" + key +
               ", cache=" + cache +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        BLPop blPop = (BLPop) o;
        return Objects.equals(key, blPop.key) && Objects.equals(cache, blPop.cache);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, cache);
    }
}
