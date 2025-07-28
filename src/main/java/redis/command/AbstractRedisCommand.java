package redis.command;

import redis.RedisSocket;
import redis.config.RedisConfig;
import redis.resp.RespValue;

import static redis.util.Logger.debug;

public abstract sealed class AbstractRedisCommand implements RedisCommand permits ConfigGet, Echo, Get, Info, Keys, PSync, Ping, ReplConf, Set, Wait {
    protected final RedisConfig config;

    protected AbstractRedisCommand(RedisConfig config) {
        this.config = config;
    }

    protected void sendResponse(RedisSocket client, RespValue response) {
        if (response != null) {
            sendResponse(client, response.serialize());
        }
    }

    protected void sendResponse(RedisSocket client, byte[] serializedResponse) {
        debug("Sending response: %s", new String(serializedResponse));
        client.write(serializedResponse);
    }
}
