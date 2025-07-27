package redis.resp;

import java.util.Deque;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static redis.resp.SerializerUtils.integerAsByteSequence;
import static redis.resp.SerializerUtils.mergeByteArrays;

public final class RespSet extends AbstractRespValue {
    private final Set<RespValue> values;

    public RespSet(Set<RespValue> values) {
        super();
        this.values = values;
    }

    public RespSet(Set<RespValue> values, int size) {
        super(size);
        this.values = values;
    }

    @Override
    public byte[] serialize() {
        if (values.isEmpty()) {
            return new byte[]{'~', '0', '\r', '\n'};
        }

        Stream<byte[]> stream = values.stream().map(RespValue::serialize);
        byte[] merged = mergeByteArrays(stream);
        Deque<Byte> lengthByteSequence = integerAsByteSequence(values.size());
        byte[] serialized = new byte[lengthByteSequence.size() + 3 + merged.length];
        serialized[0] = '~';
        int index = 1;
        while (!lengthByteSequence.isEmpty()) {
            serialized[index++] = lengthByteSequence.pop();
        }
        serialized[index++] = '\r';
        serialized[index++] = '\n';

        System.arraycopy(merged, 0, serialized, index, merged.length);
        return serialized;
    }

    public Set<RespValue> values() {
        return values;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (RespSet) obj;
        return Objects.equals(this.values, that.values) &&
               this.size() == that.size();
    }

    @Override
    public int hashCode() {
        return Objects.hash(values, size());
    }

    @Override
    public String toString() {
        return "RespSet[" +
               "values=" + values + ", " +
               "size=" + size() + ']';
    }
}
