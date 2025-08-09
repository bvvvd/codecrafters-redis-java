package redis.cache;

import redis.persistence.DumpFileReader;
import redis.persistence.NoopDumpFileReader;
import redis.persistence.PersistentFileReader;
import redis.resp.RespBulkString;
import redis.resp.RespValue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Cache {
    private static final CachedValue<RespValue> EXPIRED_ENTRY
            = new CachedValue<>(new RespBulkString(null), -1);
    private final ConcurrentMap<RespValue, CachedValue<RespValue>> data;
    private final PersistentFileReader dumpReader;

    public Cache() {
        this(new NoopDumpFileReader());
    }

    public Cache(PersistentFileReader dumpFileReader) {
        this.data = new ConcurrentHashMap<>();
        this.dumpReader = dumpFileReader;
    }

    public CachedValue<RespValue> get(RespValue key) {
        CachedValue<RespValue> cachedValue = data.computeIfPresent(key, (k, v) -> {
            if (!v.isValid()) {
                return null;
            }

            return v;
        });

        if (cachedValue != null) {
            return cachedValue;
        }
        if (key instanceof RespBulkString bulkString && bulkString.value() != null) {
            CachedValue<String> value = dumpReader.read().get(bulkString.value());
            return value != null
                    ? new CachedValue<>(new RespBulkString(value.value()), value.expirationTime())
                    : EXPIRED_ENTRY;
        }
        return EXPIRED_ENTRY;
    }

    public void put(RespValue setKey, RespValue value) {
        put(setKey, value, -1);
    }

    public void put(RespValue key, RespValue value, long expirationTime) {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key and value must not be null");
        }
        data.put(key, new CachedValue<>(value, expirationTime));
    }

    public void remove(RespValue key) {
        data.remove(key);
    }

    public List<RespValue> getPersistedKeys() {
        List<RespValue> responseValues = new ArrayList<>();
        dumpReader.read().keys().forEach(
                k -> responseValues.add(new RespBulkString(k))
        );
        return responseValues;
    }

    @Override
    public String toString() {
        return "Cache{" +
               "data=" + data +
               ", dumpReader=" + dumpReader +
               '}';
    }
}
