package redis.resp;

public final class RespNull extends AbstractRespValue {
    private static final byte[] BYTES = new byte[]{'_', '\r', '\n'};

    public RespNull() {
        super(3);
    }

    @Override
    public byte[] serialize() {
        return BYTES;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || obj != null && obj.getClass() == this.getClass();
    }

    @Override
    public int hashCode() {
        return 1;
    }

    @Override
    public String toString() {
        return "RespNull[]";
    }
}
