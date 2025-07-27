package redis.command;


import java.net.Socket;

public sealed interface RedisCommand permits AbstractRedisCommand {

    void handle(Socket client);
}
