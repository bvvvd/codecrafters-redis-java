package redis;

import redis.config.RedisConfig;

import java.net.ServerSocket;
import java.net.Socket;

import static redis.util.Logger.debug;

public class Redis implements AutoCloseable{
    private final RedisConfig config;

    public Redis(RedisConfig config) {
        this.config = config;
    }

    public void serve() {
        try (ServerSocket server = new ServerSocket(config.getPort())) {
            server.setReuseAddress(true);
            Socket client = server.accept();
        } catch (Exception e) {
            debug("Failed to start Redis server on port %d: %s%n", config.getPort(), e.getMessage());
        } finally {
            debug("Redis server has been closed.");
        }
    }

    @Override
    public void close() {
//        runFlag = false;
//        redisCoreInstance.close();
        debug("RedisCommunicator has been closed.");
    }
}
