//package redis;
//
//import redis.resp.Parser;
//import redis.resp.RespValue;
//
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.nio.channels.SocketChannel;
//import java.util.List;
//
//public class RedisReceiver {
//    private final Parser parser;
//
//    public RedisReceiver() {
//        this.parser = new Parser();
//    }
//
//    public List<RespValue> receive(SocketChannel client) throws IOException {
//        ByteBuffer buffer = ByteBuffer.allocate(256);
//        int bytesRead = client.read(buffer);
//
//        return bytesRead < 1
//                ? List.of()
//                : parser.parse(buffer.array());
//    }
//}
