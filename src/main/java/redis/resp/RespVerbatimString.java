package redis.resp;

import java.util.Deque;
import java.util.Objects;

import static redis.resp.SerializerUtils.integerAsByteSequence;

public final class RespVerbatimString extends AbstractRespValue {
    private final String encoding;
    private final String value;

    public RespVerbatimString(String encoding, String value) {
        super();
        this.encoding = encoding;
        this.value = value;
    }

    public RespVerbatimString(String encoding, String value, int size) {
        super(size);
        this.encoding = encoding;
        this.value = value;
    }

    @Override
    public byte[] serialize() {
        byte[] valueBytes = value.getBytes();
        int length = valueBytes.length;
        Deque<Byte> lengthBytes = integerAsByteSequence(length + (long) 4);
        byte[] serialized = new byte[5 + lengthBytes.size() + 4 + valueBytes.length];
        serialized[0] = '=';
        int index = 1;
        while (!lengthBytes.isEmpty()) {
            serialized[index++] = lengthBytes.pop();
        }
        serialized[index++] = '\r';
        serialized[index++] = '\n';

        for (byte b : encoding.getBytes()) {
            serialized[index++] = b;
        }
        serialized[index++] = ':';
        for (byte b : valueBytes) {
            serialized[index++] = b;
        }

        serialized[index++] = '\r';
        serialized[index++] = '\n';

        return serialized;
    }

    public String encoding() {
        return encoding;
    }

    public String value() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (RespVerbatimString) obj;
        return Objects.equals(this.encoding, that.encoding) &&
               Objects.equals(this.value, that.value) &&
               this.getSize() == that.getSize();
    }

    @Override
    public int hashCode() {
        return Objects.hash(encoding, value, getSize());
    }

    @Override
    public String toString() {
        return "RespVerbatimString[" +
               "encoding=" + encoding + ", " +
               "value=" + value + ", " +
               "size=" + getSize() + ']';
    }
}
