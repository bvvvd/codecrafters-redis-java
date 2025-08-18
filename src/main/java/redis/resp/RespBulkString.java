package redis.resp;

import java.util.Deque;
import java.util.Objects;

import static redis.resp.SerializerUtils.integerAsByteSequence;

public final class RespBulkString extends AbstractRespValue {
    private final String value;

    public RespBulkString(String value) {
        super();
        this.value = value;
    }

    public RespBulkString(String value, int size) {
        super(size);
        this.value = value;
    }

    @Override
    public byte[] serialize() {
        if (value == null) {
            return new byte[]{'$', '-', '1', '\r', '\n'};
        }
        if (value.isBlank()) {
            return new byte[]{'$', '0', '\r', '\n', '\r', '\n'};
        }
        byte[] valueBytes = value.getBytes();
        int length = valueBytes.length;
        Deque<Byte> lengthBytes = integerAsByteSequence(length);
        byte[] serialized = new byte[5 + lengthBytes.size() + valueBytes.length];
        serialized[0] = '$';
        int index = 1;
        while (!lengthBytes.isEmpty()) {
            serialized[index++] = lengthBytes.pop();
        }
        serialized[index++] = '\r';
        serialized[index++] = '\n';

        for (byte b : valueBytes) {
            serialized[index++] = b;
        }

        serialized[index++] = '\r';
        serialized[index++] = '\n';

        return serialized;
    }

    public String value() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (RespBulkString) obj;
        return Objects.equals(this.value, that.value) &&
               this.getSize() == that.getSize();
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, getSize());
    }

    @Override
    public String toString() {
        return "RespBulkString[" +
               "value=" + value + ", " +
               "size=" + getSize() + ']';
    }

    public RespInteger toRespInteger() {
        try {
            return new RespInteger(Long.parseLong(value));
        } catch (NumberFormatException _) {
            return new RespInteger(0);
        }
    }
}
