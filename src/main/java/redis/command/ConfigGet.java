package redis.command;

import redis.RedisSocket;
import redis.config.RedisConfig;
import redis.exception.RedisException;
import redis.replication.ReplicationService;
import redis.resp.RespArray;
import redis.resp.RespBulkString;
import redis.resp.RespValue;

import java.util.List;
import java.util.Objects;

import static redis.util.Logger.debug;

public final class ConfigGet extends AbstractRedisCommand {
    public static final String CODE = "CONFIG";
    private final String pattern;

    public ConfigGet(List<RespValue> tokens, RedisConfig config, ReplicationService replicationService) {
        super(config, replicationService);
        if (tokens.size() < 2 || !(tokens.get(1) instanceof RespBulkString respBulkString)
            || !respBulkString.value().equalsIgnoreCase(Get.CODE)) {
            throw new RedisException("CONFIG command requires a valid subcommand");

        }
        if (tokens.size() == 2 || !(tokens.get(2) instanceof RespBulkString patternResp)
            || patternResp.value().isBlank()
            || !(patternResp.value().equalsIgnoreCase("dbfilename") || patternResp.value().equalsIgnoreCase("dir"))) {
            throw new RedisException("CONFIG GET command requires a valid pattern argument");
        }

        this.pattern = patternResp.value();
    }

    @Override
    public void handleCommand(RedisSocket client) {
        RespArray response = new RespArray(List.of(
                new RespBulkString(pattern),
                new RespBulkString(pattern.equalsIgnoreCase("dir")
                        ? config.getDir()
                        : config.getDbFileName())));
        debug("CONFIG GET command received, returning configuration.");
        sendResponse(client, response);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (ConfigGet) obj;
        return Objects.equals(this.pattern, that.pattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pattern);
    }

    @Override
    public String toString() {
        return "ConfigGet[" +
               "pattern=" + pattern + ']';
    }
}
