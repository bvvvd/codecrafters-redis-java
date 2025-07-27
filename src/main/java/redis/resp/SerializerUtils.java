package redis.resp;

import redis.resp.exception.InternalSerializationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

public class SerializerUtils {

    private SerializerUtils() {

    }

    public static Deque<Byte> integerAsByteSequence(long valueToSerialize) {
        if (valueToSerialize == 0) {
            return new LinkedList<>(List.of((byte) 0));
        }
        if (valueToSerialize < 1) {
            throw new InternalSerializationException("Value must be positive for serialization");
        }

        Deque<Byte> bytes = new LinkedList<>();
        while (valueToSerialize > 0) {
            bytes.push((byte) (valueToSerialize % 10 + '0'));
            valueToSerialize /= 10;
        }
        return bytes;
    }

    public static byte[] mergeByteArrays(Stream<byte[]> stream) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            stream.forEach(array -> {
                try {
                    outputStream.write(array);
                } catch (IOException e) {
                    throw new InternalSerializationException("Failed to write byte array to output stream", e);
                }
            });
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new InternalSerializationException(e.getMessage(), e);
        }
    }
}
