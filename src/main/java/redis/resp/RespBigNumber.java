package redis.resp;

import java.math.BigDecimal;
import java.util.Objects;

public final class RespBigNumber extends AbstractRespValue {
    private final BigDecimal value;

    public RespBigNumber(BigDecimal value) {
        this.value = value;
    }

    public RespBigNumber(BigDecimal value, int size) {
        super(size);
        this.value = value;
    }

    @Override
    public byte[] serialize() {
        byte[] bytes = value.toString().getBytes();
        byte[] serialized = new byte[bytes.length + 3];
        serialized[0] = '(';
        System.arraycopy(bytes, 0, serialized, 1, bytes.length);
        serialized[serialized.length - 2] = '\r';
        serialized[serialized.length - 1] = '\n';
        return serialized;
    }

    public BigDecimal value() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (RespBigNumber) obj;
        return Objects.equals(this.value, that.value) &&
               this.size() == that.size();
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, size());
    }

    @Override
    public String toString() {
        return "RespBigNumber[" +
               "value=" + value + ", " +
               "size=" + size() + ']';
    }
}
