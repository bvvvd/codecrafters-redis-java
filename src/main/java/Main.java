import redis.MainEventLoop;
import redis.cache.Cache;
import redis.config.RedisConfig;
import redis.persistence.DumpFileReader;

import static redis.util.Logger.error;

public class Main {
    public static void main(String[] args) {
        RedisConfig config = new RedisConfig(args);
        DumpFileReader dumpFileReader = new DumpFileReader(config);
        Cache cache = new Cache(dumpFileReader);
        try (MainEventLoop loop = new MainEventLoop(config, cache)) {
            loop.serve();
        } catch (Exception e) {
            error("Failed to start EventLoop: %s%n", e.getMessage());
        }
    }
}
