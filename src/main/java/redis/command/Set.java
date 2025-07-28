package redis.command;

import redis.RedisSocket;
import redis.cache.CachedValue;
import redis.config.RedisConfig;
import redis.exception.RedisException;
import redis.replication.ReplicationService;
import redis.resp.RespBulkString;
import redis.resp.RespValue;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

import static redis.config.Constants.EXPIRATION_TOKEN_CONTENT;
import static redis.util.Logger.debug;

public final class Set extends AbstractRedisCommand {
    private final RespValue key;
    private final RespValue value;
    private final long expirationTime;
    private final byte[] originalBytes;
    private final ConcurrentMap<RespValue, CachedValue<RespValue>> cache;
    private final ReplicationService replicationService;

    public Set(List<RespValue> tokens, byte[] originalBytes,
               RedisConfig config, ConcurrentMap<RespValue, CachedValue<RespValue>> cache, ReplicationService replicationService) {
        super(config);
        this.cache = cache;
        this.replicationService = replicationService;

        if (tokens.size() >= 3 && (tokens.get(1) instanceof RespBulkString keyResp)
            && (tokens.get(2) instanceof RespBulkString valueResp)) {

            this.key = keyResp;
            this.value = valueResp;
            this.expirationTime = getExpirationTime(tokens);
            this.originalBytes = originalBytes;
        } else {
            throw new RedisException("SET command requires valid key and value arguments");
        }
    }

    private long getExpirationTime(List<RespValue> tokens) {
        if (tokens.size() >= 4) {
            if (tokens.get(3) instanceof RespBulkString respBulkString
                && !respBulkString.value().equalsIgnoreCase(EXPIRATION_TOKEN_CONTENT)) {
                throw new RedisException("unrecognized set option: " + respBulkString.value());
            }

            if (tokens.size() == 4 || !(tokens.get(4) instanceof RespBulkString respBulkString)) {
                throw new RedisException("SET command requires a valid expiration if px is specified");
            }

            try {
                long expiration = Long.parseLong(respBulkString.value());
                if (expiration <= 0) {
                    throw new RedisException("SET command requires a positive expiration time");
                }
                return expiration + System.currentTimeMillis();
            } catch (NumberFormatException _) {
                throw new RedisException("SET command requires a valid number for expiration time");
            }
        }
        return -1;
    }

    @Override
    public void handle(RedisSocket client) {
        boolean isMaster = config.getRole().equalsIgnoreCase("master");
        if (isMaster) {
            debug("SET command received, caching %s", this);
        } else {
            debug("Replica SET received, caching: %s", this);
        }
        cache.put(key, new CachedValue<>(value, expirationTime));
        if (isMaster) {
            RespBulkString response = new RespBulkString("OK");
            replicationService.propagate(this);

            sendResponse(client, response);
        } else {
            replicationService.moveOffset(originalBytes.length);
        }
    }

    public byte[] getOriginalBytes() {
        return originalBytes;
    }

    @Override
    public String toString() {
        return "Set{" +
               "key=" + key +
               ", value=" + value +
               ", expirationTime=" + expirationTime +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Set set = (Set) o;
        return expirationTime == set.expirationTime
               && Objects.equals(key, set.key)
               && Objects.equals(value, set.value)
               && Objects.deepEquals(originalBytes, set.originalBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, expirationTime, Arrays.hashCode(originalBytes));
    }

    public RespValue key() {
        return key;
    }

    public RespValue value() {
        return value;
    }

    public long expirationTime() {
        return expirationTime;
    }

    public byte[] originalBytes() {
        return originalBytes;
    }
}
