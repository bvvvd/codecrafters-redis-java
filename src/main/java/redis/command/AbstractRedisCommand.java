package redis.command;

import redis.config.RedisConfig;
import redis.resp.RespValue;

import java.io.IOException;
import java.net.Socket;

import static redis.util.Logger.debug;
import static redis.util.Logger.error;

public abstract sealed class AbstractRedisCommand implements RedisCommand permits ConfigGet, Echo, Get, Info, Keys, PSync, Ping, ReplConf, Set, Wait {
    protected final RedisConfig config;

    protected AbstractRedisCommand(RedisConfig config) {
        this.config = config;
    }

    protected void sendResponse(Socket client, RespValue response) {
        if (response != null) {
            try {
                byte[] serializedResponse = response.serialize();
                sendResponse(client, serializedResponse);
            } catch (IOException e) {
                error("Failed to send response: %s%n", e.getMessage());
            }
        }
    }

    protected void sendResponse(Socket client, byte[] serializedResponse) throws IOException {
        debug("Sending response: %s", new String(serializedResponse));
        client.getOutputStream().write(serializedResponse);
    }
}
