package redis.resp;

public sealed interface RespValue permits AbstractRespValue {

    byte[] serialize();

    int getSize();
}
