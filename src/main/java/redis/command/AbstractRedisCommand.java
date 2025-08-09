package redis.command;

import redis.RedisSocket;
import redis.config.RedisConfig;
import redis.replication.ReplicationService;
import redis.resp.RespBulkError;
import redis.resp.RespValue;

import static redis.util.Logger.debug;

public abstract sealed class AbstractRedisCommand implements RedisCommand permits BLPop, ConfigGet, Echo, Get, Info, Keys, LLen, LPop, LPush, LRange, PSync, Ping, RPush, ReplConf, Set, Wait {
    protected final RedisConfig config;
    protected final ReplicationService replicationService;

    protected AbstractRedisCommand(RedisConfig config, ReplicationService replicationService) {
        this.config = config;
        this.replicationService = replicationService;
    }

    protected abstract void handleCommand(RedisSocket client);

    @Override
    public final void handle(RedisSocket client) {
        try {
            handleCommand(client);
            replicationService.setLastCommand(this);
        } catch (Exception e) {
            debug("Error handling command: %s", e.getMessage());
            sendResponse(client, new RespBulkError(e.getMessage()));
        }
    }

    protected void sendResponse(RedisSocket client, RespValue response) {
        if (response != null) {
            sendResponse(client, response.serialize());
        }
    }

    protected void sendResponse(RedisSocket client, byte[] serializedResponse) {
        debug("%s, Sending response: %s", client, new String(serializedResponse));
        try {
            client.write(serializedResponse);
        } catch (Exception e) {
            debug("Error sending response: %s", e);
            throw new RuntimeException("Failed to send response to client", e);
        }
    }
}
