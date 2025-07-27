package redis.exception;

public class RedisException extends RuntimeException {

    public RedisException(String message) {
        super(message);
    }

    public RedisException(Throwable e) {
        super(e);
    }
}
