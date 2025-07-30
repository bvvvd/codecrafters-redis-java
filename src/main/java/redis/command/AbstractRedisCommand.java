package redis.command;

import redis.RedisSocket;
import redis.config.RedisConfig;
import redis.replication.ReplicationService;
import redis.resp.RespValue;

import static redis.util.Logger.debug;

public abstract sealed class AbstractRedisCommand implements RedisCommand permits ConfigGet, Echo, Get, Info, Keys, LLen, LPush, LRange, PSync, Ping, RPush, ReplConf, Set, Wait {
    protected final RedisConfig config;
    protected final ReplicationService replicationService;

    protected AbstractRedisCommand(RedisConfig config, ReplicationService replicationService) {
        this.config = config;
        this.replicationService = replicationService;
    }

    protected abstract void handleCommand(RedisSocket client);

    @Override
    public final void handle(RedisSocket client) {
        handleCommand(client);
        replicationService.setLastCommand(this);
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
