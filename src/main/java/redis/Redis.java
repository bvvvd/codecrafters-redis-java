package redis;

import redis.command.RedisCommand;
import redis.config.RedisConfig;
import redis.exception.RedisException;
import redis.replication.ReplicationService;
import redis.resp.Parser;
import redis.resp.RespValue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static redis.util.Logger.debug;
import static redis.util.Logger.error;

public class Redis implements AutoCloseable {
    private final RedisConfig config;
    private final ExecutorService clientListeners;
    private final Parser parser;
    //    private final RedisReceiver receiver;
    private final RedisCommandBuilder commandBuilder;
    private final ReplicationService replicationService;

    public Redis(RedisConfig config) {
        this.config = config;
        this.clientListeners = Executors.newVirtualThreadPerTaskExecutor();
        this.parser = new Parser();
//        this.receiver = new RedisReceiver();
        this.replicationService = new ReplicationService(Executors.newVirtualThreadPerTaskExecutor(), 0, config);
        this.commandBuilder = new RedisCommandBuilder(config, new ConcurrentHashMap<>(), replicationService);
    }

    public void serve() throws IOException {
        if (config.getRole().equalsIgnoreCase("slave")) {
            replicationService.establish();
        }

        try (ServerSocketChannel serverChannel = ServerSocketChannel.open()) {
            serverChannel.socket().bind(new InetSocketAddress(config.getPort()));
            serverChannel.socket().setReuseAddress(true);
            serverChannel.configureBlocking(true);
            while (!Thread.currentThread().isInterrupted()) {
                debug("Redis %s is running on port %d", config.getRole(), config.getPort());
                RedisSocket client = new RedisSocket(serverChannel.accept());
//                client.configureBlocking(false);
                clientListeners.submit(() -> {
                    while (client.isConnected()) {
                        try {
                            List<RespValue> readValues = parser.parse(client.read(256).get());
                            List<RedisCommand> commands = commandBuilder.build(readValues);
                            commands.forEach(command -> command.handle(client));
                        } catch (Exception e) {
                            error("Error serving client: %s%n", e.getMessage());
                            throw new RedisException(e);
                        }
                    }
                });
            }
        } catch (Exception e) {
            debug("Failed to start Redis server on port %d: %s%n", config.getPort(), e.getMessage());
        } finally {
            debug("Redis server has been closed.");
        }
    }

    @Override
    public void close() {
        clientListeners.shutdown();
        replicationService.close();
        debug("Redis has been closed.");
    }
}
