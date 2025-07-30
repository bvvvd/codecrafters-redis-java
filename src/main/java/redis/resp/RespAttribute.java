package redis.resp;

import java.util.Objects;

public final class RespAttribute extends AbstractRespValue {
    private final RespMap values;
    private final RespArray metadata;

    public RespAttribute(RespMap values, RespArray metadata) {
        this.values = values;
        this.metadata = metadata;
    }

    public RespAttribute(RespMap values, RespArray metadata, int size) {
        super(size);
        this.values = values;
        this.metadata = metadata;
    }

    @Override
    public byte[] serialize() {
        if (values.value().isEmpty()) {
            return new byte[]{'|', '0', '\r', '\n'};
        }
        byte[] serializedValues = values.serialize();
        serializedValues[0] = '|';
        byte[] serializedMetadata = metadata.serialize();
        byte[] serialized = new byte[serializedValues.length + serializedMetadata.length];
        System.arraycopy(serializedValues, 0, serialized, 0, serializedValues.length);
        System.arraycopy(serializedMetadata, 0, serialized, serializedValues.length, serializedMetadata.length);
        return serialized;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (RespAttribute) obj;
        return Objects.equals(this.values, that.values) &&
               Objects.equals(this.metadata, that.metadata) &&
               this.getSize() == that.getSize();
    }

    @Override
    public int hashCode() {
        return Objects.hash(values, metadata, getSize());
    }

    @Override
    public String toString() {
        return "RespAttribute[" +
               "values=" + values + ", " +
               "metadata=" + metadata + ", " +
               "size=" + getSize() + ']';
    }
}
