package redis.command;

import redis.config.RedisConfig;
import redis.resp.RespBulkString;

import java.net.Socket;

import static redis.util.Logger.debug;

public final class Info extends AbstractRedisCommand {
    public Info(RedisConfig config) {
        super(config);
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

    @Override
    public void handle(Socket client) {
        debug("Received INFO command");
        RespBulkString response = config.getRole().equalsIgnoreCase("master")
                ? new RespBulkString("role:%s\r\nmaster_repl_offset:0\r\nmaster_replid:%s".formatted(config.getRole(), config.getReplicationId()))
                : new RespBulkString("role:%s".formatted(config.getRole()));
        sendResponse(client, response);
    }
}
