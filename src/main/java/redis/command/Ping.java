package redis.command;

import redis.RedisSocket;
import redis.config.RedisConfig;
import redis.resp.RespSimpleString;

import static redis.config.Constants.PONG;
import static redis.util.Logger.debug;

public final class Ping extends AbstractRedisCommand {
    public Ping(RedisConfig config) {
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
        return "Ping[]";
    }


    @Override
    public void handle(RedisSocket client) {
        debug("Received PING command, sending PONG response.");
        if (config.getRole().equalsIgnoreCase("master")) {
            sendResponse(client, new RespSimpleString(PONG));
        }
    }
}
