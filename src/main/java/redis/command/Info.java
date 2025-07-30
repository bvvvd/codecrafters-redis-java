package redis.command;

import redis.RedisSocket;
import redis.config.RedisConfig;
import redis.replication.ReplicationService;
import redis.resp.RespBulkString;

import static redis.util.Logger.debug;

public final class Info extends AbstractRedisCommand {
    public static final String CODE = "INFO";

    public Info(RedisConfig config, ReplicationService replicationService) {
        super(config, replicationService);
    }

    @Override
    public void handleCommand(RedisSocket client) {
        debug("Received INFO command");
        RespBulkString response = config.getRole().equalsIgnoreCase("master")
                ? new RespBulkString("role:%s\r\nmaster_repl_offset:0\r\nmaster_replid:%s".formatted(config.getRole(), config.getReplicationId()))
                : new RespBulkString("role:%s".formatted(config.getRole()));
        sendResponse(client, response);
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || obj != null && obj.getClass() == this.getClass();
    }

    @Override
    public int hashCode() {
        return 1;
    }

    @Override
    public String toString() {
        return "Info[]";
    }
}
