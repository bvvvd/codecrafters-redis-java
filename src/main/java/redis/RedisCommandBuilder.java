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

import static redis.config.Constants.*;

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
                case PING -> new Ping(config);
                case ECHO_COMMAND_CONTENT -> new Echo(tokens, config);
                case SET_COMMAND_CONTENT -> new Set(tokens, array.serialize(), config, cache, replicationService);
                case GET_COMMAND_CONTENT -> new Get(tokens, cache, config);
                case CONFIG_COMMAND_PREFIX_CONTENT -> new ConfigGet(tokens, config);
                case KEYS_COMMAND_CONTENT -> new Keys(tokens, config);
                case INFO_COMMAND_PREFIX_CONTENT -> new Info(config);
                case REPLCONF_COMMAND_PREFIX_CONTENT -> new ReplConf(tokens, config, replicationService);
                case PSYNC_COMMAND_PREFIX_CONTENT -> new PSync(config, replicationService);
                case WAIT_COMMAND_PREFIX_CONTENT -> new Wait(tokens, config);
                default -> throw new RedisException("unsupported command: " + respBulkString.value());
            };
        }
        throw new RedisException("invalid command type: " + commandIdentifier.getClass().getSimpleName());
    }
}
