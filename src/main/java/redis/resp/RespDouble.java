package redis.resp;

import java.util.Objects;

public final class RespDouble extends AbstractRespValue {
    private final double value;

    public RespDouble(double value) {
        super();
        this.value = value;
    }

    public RespDouble(double value, int size) {
        super(size);
        this.value = value;
    }

    @Override
    public byte[] serialize() {
        if (Double.isNaN(value)) {
            return new byte[]{',', 'n', 'a', 'n', '\r', '\n'};
        }

        if (Double.isInfinite(value)) {
            if (value > 0) {
                return new byte[]{',', 'i', 'n', 'f', '\r', '\n'};
            } else {
                return new byte[]{',', '-', 'i', 'n', 'f', '\r', '\n'};
            }
        }

        String doubleString = String.valueOf(value);
        byte[] serialized = new byte[doubleString.length() + 3];
        serialized[0] = ',';
        int index = 1;
        for (char c : doubleString.toCharArray()) {
            serialized[index++] = (byte) c;
        }
        serialized[index++] = '\r';
        serialized[index] = '\n';
        return serialized;
    }

    public double value() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (RespDouble) obj;
        return Double.doubleToLongBits(this.value) == Double.doubleToLongBits(that.value) &&
               this.size() == that.size();
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, size());
    }

    @Override
    public String toString() {
        return "RespDouble[" +
               "value=" + value + ", " +
               "size=" + size() + ']';
    }
}
