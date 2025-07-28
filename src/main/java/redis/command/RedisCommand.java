package redis.command;


import redis.RedisSocket;

public sealed interface RedisCommand permits AbstractRedisCommand {

    void handle(RedisSocket client);
}
