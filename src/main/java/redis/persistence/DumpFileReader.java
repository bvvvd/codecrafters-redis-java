package redis.persistence;

import redis.cache.CachedValue;
import redis.config.RedisConfig;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static redis.util.Logger.*;

public class DumpFileReader implements PersistentFileReader {
    private static final PersistentFileReader FALLBACK_READER = new NoopDumpFileReader();
    private final RedisConfig config;

    public DumpFileReader(RedisConfig redisConfig) {
        this.config = redisConfig;
        debug("Created DumpFileReader with RedisConfig: %s%n", redisConfig);
    }

    @Override
    public DumpFileContent read() {
        String filePath = "%s/%s".formatted(config.getDir(), config.getDbFileName());
        try {
            BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(filePath));
            debug("reading a file: %s", filePath);
            return readInternally(inputStream);
        } catch (IOException e) {
            error("Failed to read dump file '%s': %s%n", filePath, e.getMessage());
            return FALLBACK_READER.read();
        }
    }

    private DumpFileContent readInternally(BufferedInputStream inputStream) throws IOException {
        String filePath = "%s/%s".formatted(config.getDir(), config.getDbFileName());
        skipHeader(inputStream);
        Map<String, CachedValue<String>> cache = new HashMap<>();
        try {
            loopThroughFile(inputStream, cache);
        } catch (EOFException _) {
            warn("Unexpected end of file while reading dump file '%s'.%n", filePath);
        }
        return new DumpFileContent(cache);
    }

    private void loopThroughFile(BufferedInputStream inputStream, Map<String, CachedValue<String>> keys) throws IOException {
        while (true) {
            int code = inputStream.read();
            if (code == -1) {
                break;
            }

            switch (code) {
                case 0x00 -> {
                    String key = readString(inputStream);
                    if (!key.equalsIgnoreCase("Unsupported String Encoding")) {
                        String value = readString(inputStream);
                        keys.put(key, new CachedValue<>(value, -1));
                    }
                }
                case 0xfc -> {
                    long expiry = readExpiryMilliseconds(inputStream);
                    inputStream.read();

                    String key = readString(inputStream);
                    if (!key.equalsIgnoreCase("Unsupported String Encoding")) {
                        String value = readString(inputStream);
                        keys.put(key, new CachedValue<>(value, expiry));
                    }
                }
                case 0xfd -> {
                    long expiry = readExpirySeconds(inputStream);
                    inputStream.read();
                    String key = readString(inputStream);
                    if (!key.equalsIgnoreCase("Unsupported String Encoding")) {
                        String value = readString(inputStream);
                        keys.put(key, new CachedValue<>(value, expiry));
                    }
                }
                case 0xfa -> skipAuxField(inputStream);
                case 0xfe -> skipSelectDb(inputStream);
                case 0xfb -> skipResizeDb(inputStream);
                default -> skipUnknown(inputStream);
            }
        }
    }

    private long readExpiryMilliseconds(BufferedInputStream inputStream) throws IOException {
        return (inputStream.read())
               | ((long) inputStream.read() << 8)
               | ((long) inputStream.read() << 16)
               | ((long) inputStream.read() << 24)
               | ((long) inputStream.read() << 32)
               | ((long) inputStream.read() << 40)
               | ((long) inputStream.read() << 48)
               | ((long) inputStream.read() << 56);
    }

    private long readExpirySeconds(BufferedInputStream inputStream) throws IOException {
        long decodedExpiration = (inputStream.read())
                                 | ((long) inputStream.read() << 8)
                                 | ((long) inputStream.read() << 16)
                                 | ((long) inputStream.read() << 24);
        return decodedExpiration * 1000;
    }

    private void skipHeader(BufferedInputStream inputStream) throws IOException {
        inputStream.skip(9);
    }

    private void skipAuxField(BufferedInputStream inputStream) throws IOException {
        readString(inputStream);
        readString(inputStream);
    }

    private void skipSelectDb(BufferedInputStream inputStream) throws IOException {
        inputStream.read();
    }

    private void skipResizeDb(BufferedInputStream inputStream) throws IOException {
        readLength(inputStream);
        readLength(inputStream);
    }

    private void skipMetadata(BufferedInputStream inputStream) throws IOException {
        int opcode = inputStream.read();
        switch (opcode) {
            case 8 -> inputStream.skip(8); // Skip 64-bit expiry
            case 4 -> inputStream.skip(4); // Skip 32-bit expiry
            default -> inputStream.skip(1); // fallback skip
        }
    }

    private String readString(BufferedInputStream inputStream) throws IOException {
        int first = inputStream.read();
        if (first == -1) {
            throw new EOFException("Unexpected end of file while reading string");
        }
        if ((first & 0xC0) == 0x00) {
            // 6-bit length
            return readString(inputStream, first);
        } else if ((first & 0xC0) == 0x40) {
            // 14-bit length
            int second = inputStream.read();
            int len = ((first & 0x3F) << 8) | second;
            return readString(inputStream, len);
        } else if ((first & 0xC0) == 0x80) {
            String string = Integer.toString(inputStream.read());
            return string;
        } else if (first == 0xC0) {
            String string = Integer.toString(inputStream.read());
            return string;
        } else if (first == 0xC1) {
            // 16-bit length
            int left = inputStream.read();
            int right = inputStream.read();
            int readInt = (left << 8) | right;
            String string = Integer.toString(readInt);
            return string;
        } else if (first == 0xC2) {
            // 32-bit length
            int left = inputStream.read();
            int middle = inputStream.read();
            int right = inputStream.read();
            int last = inputStream.read();
            int readInt = (left << 24) | (middle << 16) | (right << 8) | last;
            String string = Integer.toString(readInt);
            return string;
        } else {
            warn("Unsupported string encoding: 0x%02X%n", first);
            return "Unsupported String Encoding";
        }
    }

    private String readString(BufferedInputStream inputStream, int length) throws IOException {
        String readString = new String(inputStream.readNBytes(length));
        return readString;
    }

    private void skipUnknown(BufferedInputStream inputStream) throws IOException {
        inputStream.skip(1);
    }

    private long readLength(BufferedInputStream inputStream) throws IOException {
        int first = inputStream.read();
        if ((first & 0xC0) == 0x00) return first;
        else if ((first & 0xC0) == 0x40) return ((first & 0x3F) << 8) | inputStream.read();
        else if ((first & 0xC0) == 0x80)
            return ((long) inputStream.read() << 24) | (inputStream.read() << 16) | (inputStream.read() << 8) | inputStream.read();
        else throw new IOException("Unsupported length encoding");
    }
}
