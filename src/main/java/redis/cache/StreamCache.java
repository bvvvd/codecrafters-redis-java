package redis.cache;

import redis.resp.RespArray;
import redis.resp.RespBulkString;
import redis.resp.RespValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamCache {
    private final Map<RespValue, RedisStream> streams;

    public StreamCache() {
        streams = new HashMap<>();
    }

    public boolean containsKey(RespValue key) {
        return streams.containsKey(key);
    }

    public RespValue add(RespValue key, RespBulkString entryId, List<RespValue> streamValues) {
        if (!containsKey(key)) {
            streams.put(key, new RedisStream());
        }
        return streams.get(key).append(entryId, streamValues);
    }

    public RespValue range(RespValue key, String start, String end) {
        RedisStream stream = streams.get(key);
        if (stream == null) {
            return new RespArray(List.of());
        }
        return stream.range(start, end);
    }
}
