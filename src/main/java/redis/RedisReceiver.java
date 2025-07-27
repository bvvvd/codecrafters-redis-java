package redis;

import redis.resp.Parser;
import redis.resp.RespValue;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

public class RedisReceiver {
    private final Parser parser;

    public RedisReceiver() {
        this.parser = new Parser();
    }

    public List<RespValue> receive(Socket client) throws IOException {
        byte[] buffer = new byte[256];
        int bytesRead = client.getInputStream().read(buffer);

        return bytesRead < 1
                ? List.of()
                : parser.parse(buffer);
    }
}
