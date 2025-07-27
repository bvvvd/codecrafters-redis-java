package redis.resp;

import java.util.Objects;

public final class RespBoolean extends AbstractRespValue {
    private final boolean value;

    public RespBoolean(boolean value) {
        this.value = value;
    }

    @Override
    public byte[] serialize() {
        return value ? new byte[]{'#', 't', '\r', '\n'} : new byte[]{'#', 'f', '\r', '\n'};
    }

    @Override
    public int size() {
        return 4;
    }

    public boolean value() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (RespBoolean) obj;
        return this.value == that.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return "RespBoolean[" +
               "value=" + value + ']';
    }
}
