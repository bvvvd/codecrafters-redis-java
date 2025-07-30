package redis.resp;

import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static redis.resp.SerializerUtils.integerAsByteSequence;
import static redis.resp.SerializerUtils.mergeByteArrays;

public final class RespMap extends AbstractRespValue {
    private final Map<RespValue, RespValue> value;

    public RespMap(Map<RespValue, RespValue> value) {
        super();
        this.value = value;
    }

    public RespMap(Map<RespValue, RespValue> value, int size) {
        super(size);
        this.value = value;
    }

    @Override
    public byte[] serialize() {
        int length = value.size();
        Deque<Byte> lengthBytes = integerAsByteSequence(length);
        Stream<byte[]> stream = value.entrySet().stream().flatMap(entry -> {
            RespValue key = entry.getKey();
            RespValue entryValue = entry.getValue();

            return Stream.concat(
                    Stream.of(key.serialize()),
                    Stream.of(entryValue.serialize())
            );
        });

        byte[] merged = mergeByteArrays(stream);
        byte[] serialized = new byte[3 + lengthBytes.size() + merged.length];
        serialized[0] = '%';
        int index = 1;
        for (Byte b : lengthBytes) {
            serialized[index++] = b;
        }
        serialized[index++] = '\r';
        serialized[index++] = '\n';
        System.arraycopy(merged, 0, serialized, index, merged.length);
        return serialized;
    }

    public Map<RespValue, RespValue> value() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (RespMap) obj;
        return Objects.equals(this.value, that.value) &&
               this.getSize() == that.getSize();
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, getSize());
    }

    @Override
    public String toString() {
        return "RespMap[" +
               "value=" + value + ", " +
               "size=" + getSize() + ']';
    }
}
