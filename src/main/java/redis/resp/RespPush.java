package redis.resp;

import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static redis.resp.SerializerUtils.integerAsByteSequence;
import static redis.resp.SerializerUtils.mergeByteArrays;

public final class RespPush extends AbstractRespValue {
    private final List<RespValue> values;

    public RespPush(List<RespValue> values) {
        super();
        this.values = values;
    }

    public RespPush(List<RespValue> values, int size) {
        super(size);
        this.values = values;
    }

    @Override
    public byte[] serialize() {
        if (values.isEmpty()) {
            return new byte[]{'>', '0', '\r', '\n'};
        }

        Stream<byte[]> stream = values.stream().map(RespValue::serialize);
        byte[] merged = mergeByteArrays(stream);
        Deque<Byte> lengthByteSequence = integerAsByteSequence(values.size());
        byte[] serialized = new byte[lengthByteSequence.size() + 3 + merged.length];
        serialized[0] = '>';
        int index = 1;
        while (!lengthByteSequence.isEmpty()) {
            serialized[index++] = lengthByteSequence.pop();
        }
        serialized[index++] = '\r';
        serialized[index++] = '\n';

        System.arraycopy(merged, 0, serialized, index, merged.length);
        return serialized;
    }

    public List<RespValue> values() {
        return values;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (RespPush) obj;
        return Objects.equals(this.values, that.values) &&
               this.getSize() == that.getSize();
    }

    @Override
    public int hashCode() {
        return Objects.hash(values, getSize());
    }

    @Override
    public String toString() {
        return "RespPush[" +
               "values=" + values + ", " +
               "size=" + getSize() + ']';
    }
}
