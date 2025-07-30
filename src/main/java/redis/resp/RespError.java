package redis.resp;

import java.util.Objects;

public final class RespError extends AbstractRespValue {
    private final String value;

    public RespError(String value) {
        super();
        this.value = value;
    }

    public RespError(String value, int size) {
        super(size);
        this.value = value;
    }

    @Override
    public byte[] serialize() {
        byte[] valueBytes = value.getBytes();

        byte[] serialized = new byte[valueBytes.length + 3];
        serialized[0] = '+';
        System.arraycopy(valueBytes, 0, serialized, 1, valueBytes.length);
        serialized[serialized.length - 2] = '\r';
        serialized[serialized.length - 1] = '\n';
        return serialized;
    }

    public String value() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (RespError) obj;
        return Objects.equals(this.value, that.value) &&
               this.getSize() == that.getSize();
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, getSize());
    }

    @Override
    public String toString() {
        return "RespError[" +
               "value=" + value + ", " +
               "size=" + getSize() + ']';
    }
}