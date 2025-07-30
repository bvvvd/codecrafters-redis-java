package redis.resp;

public abstract sealed class AbstractRespValue implements RespValue permits RespArray, RespAttribute, RespBigNumber, RespBoolean, RespBulkError, RespBulkString, RespDouble, RespError, RespInteger, RespMap, RespNull, RespPush, RespSet, RespSimpleString, RespVerbatimString {
    protected final int size;

    protected AbstractRespValue() {
        this.size = -1;
    }

    protected AbstractRespValue(int size) {
        this.size = size;
    }

    @Override
    public int getSize() {
        if (size < 0) {
            return serialize().length;
        }

        return size;
    }
}
