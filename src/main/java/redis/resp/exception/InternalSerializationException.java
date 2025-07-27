package redis.resp.exception;

public class InternalSerializationException extends RuntimeException {
    public InternalSerializationException(String message) {
        super(message);
    }

    public InternalSerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
