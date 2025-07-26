import redis.Redis;
import redis.config.RedisConfig;

import static redis.util.Logger.error;

public class Main {
    public static void main(String[] args) {
        RedisConfig config = new RedisConfig(args);
        try (Redis redis = new Redis(config)) {
            redis.serve();
        } catch (Exception e) {
            error("Failed to start Redis server: %s%n", e.getMessage());
        }
    }
}
