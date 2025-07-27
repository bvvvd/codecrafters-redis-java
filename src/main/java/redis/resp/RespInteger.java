package redis.resp;

import java.util.Deque;
import java.util.Objects;

import static redis.resp.SerializerUtils.integerAsByteSequence;

public final class RespInteger extends AbstractRespValue {
    private final long value;

    public RespInteger(long value) {
        super();
        this.value = value;
    }

    public RespInteger(long value, int size) {
        super(size);
        this.value = value;
    }

    @Override
    public byte[] serialize() {
        if (value == 0) {
            return new byte[]{':', '0', '\r', '\n'};
        }
        if (value == Long.MIN_VALUE) {
            return new byte[]{':', '-', '9', '2', '2', '3', '3', '7', '2', '0', '3', '6', '8', '5', '4', '7', '7', '5', '8', '0', '8', '\r', '\n'};
        }

        Deque<Byte> byteStack = integerAsByteSequence(Math.abs(value));
        byte[] serializedBytes;
        int index;
        if (value > 0) {
            serializedBytes = new byte[byteStack.size() + 3];
            serializedBytes[0] = ':';
            index = 1;
        } else {
            serializedBytes = new byte[byteStack.size() + 4];
            serializedBytes[0] = ':';
            serializedBytes[1] = '-';
            index = 2;
        }
        while (!byteStack.isEmpty()) {
            serializedBytes[index++] = byteStack.pop();
        }
        serializedBytes[index++] = '\r';
        serializedBytes[index] = '\n';
        return serializedBytes;
    }

    public long value() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (RespInteger) obj;
        return this.value == that.value &&
               this.size() == that.size();
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, size());
    }

    @Override
    public String toString() {
        return "RespInteger[" +
               "value=" + value + ", " +
               "size=" + size() + ']';
    }
}