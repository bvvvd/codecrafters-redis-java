package redis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Optional;

import static redis.util.Logger.debug;
import static redis.util.Logger.error;

public class RedisSocket implements AutoCloseable {
    private final SocketChannel socketChannel;

    public RedisSocket(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
        try {
            socketChannel.configureBlocking(false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isConnected() {
        return socketChannel.isConnected();
    }

    public Optional<byte[]> read(int length) {
        var buffer = ByteBuffer.allocate(length);
        var totalBytesRead = 0;
        try {
            do {
                totalBytesRead += read(buffer);
            } while (totalBytesRead > 0 && buffer.limit() > buffer.position() && buffer.get(buffer.position()-1) != (byte)'\n');
        } catch (IOException e) {
            error("Got exception while reading: %s", e.getMessage());
            close();
        }
        buffer.flip();
        var messageSize = Math.max(totalBytesRead, 0);
        var message = new byte[messageSize];
        buffer.get(message, 0, messageSize);
        if (message.length > 0) {
            debug("Received message: %s",  new String(message));
            return Optional.of(message);
        }
        return Optional.empty();
    }

    private int read(ByteBuffer buffer) throws IOException {
        var bytesRead = socketChannel.read(buffer);
        if (bytesRead > 0) {
            debug("Bytes read: %s %s", bytesRead, new String(buffer.array()));
        }
        return bytesRead;
    }

    public void write(byte[] message) {
        try {
            var buffer = ByteBuffer.allocate(message.length);
            buffer.put(message);
            buffer.flip();
            socketChannel.write(buffer);
        } catch (IOException e) {
            error("Got exception while writing: %s", e.getMessage());
        }
    }

    @Override
    public void close() {
        try {
            socketChannel.close();
        } catch (IOException e) {
            error("Got exception while closing: %s", e.getMessage());
        }
    }
}
