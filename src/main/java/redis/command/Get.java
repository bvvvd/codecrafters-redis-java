package redis.command;

import redis.RedisSocket;
import redis.cache.CachedValue;
import redis.config.RedisConfig;
import redis.exception.RedisException;
import redis.persistence.DumpFileContent;
import redis.persistence.DumpFileReader;
import redis.persistence.PersistentFileReader;
import redis.replication.ReplicationService;
import redis.resp.RespBulkString;
import redis.resp.RespValue;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

import static redis.util.Logger.debug;

public final class Get extends AbstractRedisCommand {
    private final RespBulkString key;
    private final ConcurrentMap<RespValue, CachedValue<RespValue>> cache;
    private final PersistentFileReader dumpFileReader;

    public Get(List<RespValue> tokens,
               ConcurrentMap<RespValue, CachedValue<RespValue>> cache,
               RedisConfig config, ReplicationService replicationService) {
        super(config, replicationService);
        if (tokens.size() < 2 || !(tokens.get(1) instanceof RespBulkString keyResp)) {
            throw new RedisException("GET command requires a valid key argument");
        }
        this.key = keyResp;
        this.cache = cache;
        this.dumpFileReader = new DumpFileReader(config);
    }

    @Override
    public void handleCommand(RedisSocket client) {
        debug("cache content: %s", cache);
        CachedValue<RespValue> cachedValue = cache.get(key);
        if (cachedValue == null || !cachedValue.isValid()) {
            DumpFileContent dump = dumpFileReader.read();
            debug("dump content: %s", dump);
            CachedValue<String> value = dump.get(key.value());
            cachedValue = value != null
                    ? new CachedValue<>(new RespBulkString(value.value()), value.expirationTime())
                    : new CachedValue<>(new RespBulkString(null), -1);
        }
        debug("GET command received for key '%s', returning value: %s", key, cachedValue);
        if (!cachedValue.isValid()) {
            debug("Cached value for key '%s' has expired or does not exist.", key);
            sendResponse(client, new RespBulkString(null));
        } else {
            byte[] serialized = cachedValue.getValue().serialize();
            debug("Returning value for key '%s': %s", key, new String(serialized));
            sendResponse(client, cachedValue.getValue());
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (Get) obj;
        return Objects.equals(this.key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    @Override
    public String toString() {
        return "Get[" +
               "key=" + key + ']';
    }
}
