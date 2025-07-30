package redis;

import redis.cache.CachedValue;
import redis.command.*;
import redis.config.RedisConfig;
import redis.exception.RedisException;
import redis.replication.ReplicationService;
import redis.resp.RespArray;
import redis.resp.RespBulkString;
import redis.resp.RespValue;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class RedisCommandBuilder {
    private final RedisConfig config;
    private final ConcurrentMap<RespValue, CachedValue<RespValue>> cache;
    private final ReplicationService replicationService;

    public RedisCommandBuilder(RedisConfig config, ConcurrentMap<RespValue, CachedValue<RespValue>> cache, ReplicationService replicationService) {
        this.config = config;
        this.cache = cache;
        this.replicationService = replicationService;
    }

    public List<RedisCommand> build(List<RespValue> readValues) {
        return readValues.stream().map(
                this::build
        ).toList();
    }

    private RedisCommand build(RespValue value) {
        if (!(value instanceof RespArray array) || array.values() == null || array.values().isEmpty()) {
            throw new RedisException("input array is null or empty");
        }

        List<RespValue> tokens = array.values();
        RespValue commandIdentifier = tokens.getFirst();
        if (commandIdentifier instanceof RespBulkString respBulkString) {
            return switch (respBulkString.value().toUpperCase()) {
                case Ping.CODE -> new Ping(config, replicationService);
                case Echo.CODE -> new Echo(tokens, config, replicationService);
                case Set.CODE -> new Set(tokens, array.serialize(), config, cache, replicationService);
                case Get.CODE -> new Get(tokens, cache, config, replicationService);
                case ConfigGet.CODE -> new ConfigGet(tokens, config, replicationService);
                case Keys.CODE -> new Keys(tokens, config, replicationService);
                case Info.CODE -> new Info(config, replicationService);
                case ReplConf.CODE -> new ReplConf(tokens, array.serialize().length, config, replicationService);
                case PSync.CODE -> new PSync(config, replicationService);
                case Wait.CODE -> new Wait(tokens, config, replicationService);
                case RPush.CODE -> new RPush(tokens, array.serialize(), config, cache, replicationService);
                case LRange.CODE -> new LRange(tokens, cache, config, replicationService);
                case LPush.CODE -> new LPush(tokens, array.serialize(), config, cache, replicationService);
                case LLen.CODE -> new LLen(tokens, cache, config, replicationService);
                case LPop.CODE -> new LPop(tokens, array.serialize(), cache, config, replicationService);
                default -> throw new RedisException("unsupported command: " + respBulkString.value());
            };
        }
        throw new RedisException("invalid command type: " + commandIdentifier.getClass().getSimpleName());
    }
}
