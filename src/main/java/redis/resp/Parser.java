package redis.resp;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static redis.util.Logger.debug;

public class Parser {

    public List<RespValue> parse(byte[] input) {
        if (input == null || input.length == 0) {
            throw new IllegalArgumentException("Input cannot be null or empty");
        }

        AtomicInteger offset = new AtomicInteger();
        List<RespValue> values = new ArrayList<>();
        while (offset.get() < input.length && input[offset.get()] != 0
               && input[offset.get()] != '\n' && input[offset.get()] != '\r') {
            values.add(parse(input, offset));
        }
        return values;
    }

    public List<RespValue> parse(ByteBuffer input) {
        if (input == null || input.remaining() == 0) {
            throw new IllegalArgumentException("Input cannot be null or empty " + input);
        }

        AtomicInteger offset = new AtomicInteger();
        List<RespValue> values = new ArrayList<>();
        while (offset.get() < input.remaining() && input.get(offset.get()) != 0
               && input.get(offset.get()) != '\n' && input.get(offset.get()) != '\r') {
            values.add(parse(input, offset));
        }
        return values;
    }

    private RespValue parse(byte[] input, AtomicInteger offset) {
        if (offset.get() >= input.length) {
            throw new IllegalArgumentException("Offset is out of bounds");
        }

        byte requestType = input[offset.get()];
        return switch (requestType) {
            case '+' -> simpleString(input, offset);
            case '-' -> error(input, offset);
            case ':' -> integer(input, offset);
            case '$' -> bulkString(input, offset);
            case '*' -> array(input, offset);
            case '_' -> respNull(input, offset);
            case '#' -> respBoolean(input, offset);
            case ',' -> respDouble(input, offset);
            case '(' -> bigNumber(input, offset);
            case '!' -> bulkError(input, offset);
            case '=' -> verbatimString(input, offset);
            case '%' -> map(input, offset);
            case '|' -> attributes(input, offset);
            case '~' -> set(input, offset);
            case '>' -> push(input, offset);
            default -> throw new IllegalArgumentException("Invalid RESP format " + (char) requestType);
        };
    }

    private RespValue parse(ByteBuffer input, AtomicInteger offset) {
        if (offset.get() >= input.remaining()) {
            throw new IllegalArgumentException("Offset is out of bounds");
        }

        byte requestType = input.get(offset.get());
        return switch (requestType) {
            case '+' -> simpleString(input, offset);
            case '-' -> error(input, offset);
            case ':' -> integer(input, offset);
            case '$' -> bulkString(input, offset);
            case '*' -> array(input, offset);
            case '_' -> respNull(input, offset);
            case '#' -> respBoolean(input, offset);
            case ',' -> respDouble(input, offset);
            case '(' -> bigNumber(input, offset);
            case '!' -> bulkError(input, offset);
            case '=' -> verbatimString(input, offset);
            case '%' -> map(input, offset);
            case '|' -> attributes(input, offset);
            case '~' -> set(input, offset);
            case '>' -> push(input, offset);
            default -> throw new IllegalArgumentException("Invalid RESP format " + (char) requestType);
        };
    }

    private RespValue push(byte[] input, AtomicInteger offset) {
        int start = offset.get();
        return new RespPush(array(input, offset).values(), offset.get() - start);
    }

    private RespValue push(ByteBuffer input, AtomicInteger offset) {
        int start = offset.get();
        return new RespPush(array(input, offset).values(), offset.get() - start);
    }

    private RespValue set(byte[] input, AtomicInteger offset) {
        int start = offset.get();
        int length = 0;
        offset.incrementAndGet();
        while (offset.get() + 1 < input.length && input[offset.get() + 1] != '\n'
               && input[offset.get()] != '\r') {
            if (input[offset.get()] < '0' || input[offset.get()] > '9') {
                throw new IllegalArgumentException("Invalid array length");
            }
            length = length * 10 + (input[offset.get()] - '0');
            offset.incrementAndGet();
        }
        offset.addAndGet(2);

        Set<RespValue> values = new HashSet<>();
        for (int i = 0; i < length; i++) {
            if (offset.get() >= input.length) {
                throw new IllegalArgumentException("Expected array entry");
            }
            RespValue value = parse(input, offset);
            if (!values.add(value)) {
                throw new IllegalArgumentException("Duplicate value in set");
            }
        }
        return new RespSet(values, offset.get() - start);
    }

    private RespValue set(ByteBuffer input, AtomicInteger offset) {
        int start = offset.get();
        int length = 0;
        offset.incrementAndGet();
        while (offset.get() + 1 < input.remaining() && input.get(offset.get() + 1) != '\n'
               && input.get(offset.get()) != '\r') {
            if (input.get(offset.get()) < '0' || input.get(offset.get()) > '9') {
                throw new IllegalArgumentException("Invalid array length");
            }
            length = length * 10 + (input.get(offset.get()) - '0');
            offset.incrementAndGet();
        }
        offset.addAndGet(2);

        Set<RespValue> values = new HashSet<>();
        for (int i = 0; i < length; i++) {
            if (offset.get() >= input.remaining()) {
                throw new IllegalArgumentException("Expected array entry");
            }
            RespValue value = parse(input, offset);
            if (!values.add(value)) {
                throw new IllegalArgumentException("Duplicate value in set");
            }
        }
        return new RespSet(values, offset.get() - start);
    }

    private RespAttribute attributes(byte[] input, AtomicInteger offset) {
        int start = offset.get();
        if (input[start + 1] == '0') {
            offset.addAndGet(4);
            return new RespAttribute(new RespMap(Collections.emptyMap()), new RespArray(Collections.emptyList()), 4);
        }
        RespMap map = map(input, offset);
        RespArray array = array(input, offset);
        return new RespAttribute(map, array, offset.get() - start);
    }

    private RespAttribute attributes(ByteBuffer input, AtomicInteger offset) {
        int start = offset.get();
        if (input.get(start + 1) == '0') {
            offset.addAndGet(4);
            return new RespAttribute(new RespMap(Collections.emptyMap()), new RespArray(Collections.emptyList()), 4);
        }
        RespMap map = map(input, offset);
        RespArray array = array(input, offset);
        return new RespAttribute(map, array, offset.get() - start);
    }

    private RespMap map(byte[] input, AtomicInteger offset) {
        int start = offset.get();
        int length = 0;
        offset.incrementAndGet();
        while (offset.get() + 1 < input.length && input[offset.get() + 1] != '\n'
               && input[offset.get()] != '\r') {
            if (input[offset.get()] < '0' || input[offset.get()] > '9') {
                throw new IllegalArgumentException("Invalid map length");
            }
            length = length * 10 + (input[offset.get()] - '0');
            offset.incrementAndGet();
        }
        offset.addAndGet(2);
        Map<RespValue, RespValue> map = new HashMap<>();
        for (int i = 0; i < length; i++) {
            if (offset.get() >= input.length) {
                throw new IllegalArgumentException("Expected key-value pair in map");
            }
            RespValue key = parse(input, offset);
            RespValue value = parse(input, offset);
            map.put(key, value);
        }
        return new RespMap(map, offset.get() - start);
    }

    private RespMap map(ByteBuffer input, AtomicInteger offset) {
        int start = offset.get();
        int length = 0;
        offset.incrementAndGet();
        while (offset.get() + 1 < input.remaining() && input.get(offset.get() + 1) != '\n'
               && input.get(offset.get()) != '\r') {
            if (input.get(offset.get()) < '0' || input.get(offset.get()) > '9') {
                throw new IllegalArgumentException("Invalid map length");
            }
            length = length * 10 + (input.get(offset.get()) - '0');
            offset.incrementAndGet();
        }
        offset.addAndGet(2);
        Map<RespValue, RespValue> map = new HashMap<>();
        for (int i = 0; i < length; i++) {
            if (offset.get() >= input.remaining()) {
                throw new IllegalArgumentException("Expected key-value pair in map");
            }
            RespValue key = parse(input, offset);
            RespValue value = parse(input, offset);
            map.put(key, value);
        }
        return new RespMap(map, offset.get() - start);
    }

    private RespVerbatimString verbatimString(byte[] input, AtomicInteger offset) {
        int start = offset.get();
        int length = 0;
        offset.incrementAndGet();
        while (offset.get() + 1 < input.length && input[offset.get() + 1] != '\n'
               && input[offset.get()] != '\r') {
            if (input[offset.get()] < '0' || input[offset.get()] > '9') {
                throw new IllegalArgumentException("Invalid verbatim string length");
            }
            length = length * 10 + (input[offset.get()] - '0');
            offset.incrementAndGet();
        }
        offset.addAndGet(2);
        StringBuilder builder = new StringBuilder();
        for (int i = offset.get(); i < offset.get() + length; i++) {
            if (i >= input.length || input[i] == '\r' || input[i] == '\n') {
                throw new IllegalArgumentException("Unexpected end of input " + offset + " " + (char) input[offset.get()]);
            }
            builder.append((char) input[i]);
        }
        if (offset.get() + length >= input.length
            || input[offset.get() + length] != '\r'
            || input[offset.get() + length + 1] != '\n') {
            throw new IllegalArgumentException("Bulk string must end with CRLF");
        }
        offset.addAndGet(2);
        String fullString = builder.toString();
        String[] split = fullString.split(":");
        if (split.length == 0 || fullString.length() == 4 && split[0].length() != 3 || split.length > 2) {
            throw new IllegalArgumentException("Invalid verbatim string format");
        }
        if (split.length == 1 && (split[0].length() != 3 || fullString.length() != 4)) {
            throw new IllegalArgumentException("Invalid verbatim string format");
        }
        offset.addAndGet(length);
        return new RespVerbatimString(split[0], split.length == 1 ? "" : split[1], offset.get() - start);
    }

    private RespVerbatimString verbatimString(ByteBuffer input, AtomicInteger offset) {
        int start = offset.get();
        int length = 0;
        offset.incrementAndGet();
        while (offset.get() + 1 < input.remaining() && input.get(offset.get() + 1) != '\n'
               && input.get(offset.get()) != '\r') {
            if (input.get(offset.get()) < '0' || input.get(offset.get()) > '9') {
                throw new IllegalArgumentException("Invalid verbatim string length");
            }
            length = length * 10 + (input.get(offset.get()) - '0');
            offset.incrementAndGet();
        }
        offset.addAndGet(2);
        StringBuilder builder = new StringBuilder();
        for (int i = offset.get(); i < offset.get() + length; i++) {
            if (i >= input.remaining() || input.get(i) == '\r' || input.get(i) == '\n') {
                throw new IllegalArgumentException("Unexpected end of input " + offset + " " + input.get(offset.get()));
            }
            builder.append((char) input.get(i));
        }
        if (offset.get() + length >= input.remaining()
            || input.get(offset.get() + length) != '\r'
            || input.get(offset.get() + length + 1) != '\n') {
            throw new IllegalArgumentException("Bulk string must end with CRLF");
        }
        offset.addAndGet(2);
        String fullString = builder.toString();
        String[] split = fullString.split(":");
        if (split.length == 0 || fullString.length() == 4 && split[0].length() != 3 || split.length > 2) {
            throw new IllegalArgumentException("Invalid verbatim string format");
        }
        if (split.length == 1 && (split[0].length() != 3 || fullString.length() != 4)) {
            throw new IllegalArgumentException("Invalid verbatim string format");
        }
        offset.addAndGet(length);
        return new RespVerbatimString(split[0], split.length == 1 ? "" : split[1], offset.get() - start);
    }

    private RespBulkError bulkError(byte[] input, AtomicInteger offset) {
        int start = offset.get();
        int length = 0;
        offset.incrementAndGet();
        while (offset.get() + 1 < input.length && input[offset.get() + 1] != '\n'
               && input[offset.get()] != '\r') {
            if (input[offset.get()] < '0' || input[offset.get()] > '9') {
                throw new IllegalArgumentException("Invalid bulk error string length");
            }
            length = length * 10 + (input[offset.get()] - '0');
            offset.incrementAndGet();
        }
        offset.addAndGet(2);
        StringBuilder builder = new StringBuilder();
        for (int i = offset.get(); i < offset.get() + length; i++) {
            if (i >= input.length || input[i] == '\r' || input[i] == '\n') {
                throw new IllegalArgumentException("Unexpected end of input " + offset + " " + (char) input[offset.get()]);
            }
            builder.append((char) input[i]);
        }
        if (offset.get() + length >= input.length
            || input[offset.get() + length] != '\r'
            || input[offset.get() + length + 1] != '\n') {
            throw new IllegalArgumentException("Bulk string must end with CRLF");
        }
        offset.addAndGet(length + 2);
        return new RespBulkError(builder.toString(), offset.get() - start);
    }

    private RespBulkError bulkError(ByteBuffer input, AtomicInteger offset) {
        int start = offset.get();
        int length = 0;
        offset.incrementAndGet();
        while (offset.get() + 1 < input.remaining() && input.get(offset.get() + 1) != '\n'
               && input.get(offset.get()) != '\r') {
            if (input.get(offset.get()) < '0' || input.get(offset.get()) > '9') {
                throw new IllegalArgumentException("Invalid bulk error string length");
            }
            length = length * 10 + (input.get(offset.get()) - '0');
            offset.incrementAndGet();
        }
        offset.addAndGet(2);
        StringBuilder builder = new StringBuilder();
        for (int i = offset.get(); i < offset.get() + length; i++) {
            if (i >= input.remaining() || input.get(i) == '\r' || input.get(i) == '\n') {
                throw new IllegalArgumentException("Unexpected end of input " + offset + " " + input.get(offset.get()));
            }
            builder.append((char) input.get(i));
        }
        if (offset.get() + length >= input.remaining()
            || input.get(offset.get() + length) != '\r'
            || input.get(offset.get() + length + 1) != '\n') {
            throw new IllegalArgumentException("Bulk string must end with CRLF");
        }
        offset.addAndGet(length + 2);
        return new RespBulkError(builder.toString(), offset.get() - start);
    }

    private RespBigNumber bigNumber(byte[] input, AtomicInteger offset) {
        int start = offset.get();
        StringBuilder builder = new StringBuilder();
        offset.incrementAndGet();
        while (offset.get() + 1 < input.length && input[offset.get() + 1] != '\n'
               && input[offset.get()] != '\r') {
            if (input[offset.get()] < '0' || input[offset.get()] > '9') {
                throw new IllegalArgumentException("Invalid big number format");
            }
            builder.append((char) input[offset.get()]);
            offset.incrementAndGet();
        }
        offset.addAndGet(2);
        String stringValue = builder.toString();
        return new RespBigNumber(new BigDecimal(stringValue), offset.get() - start);
    }

    private RespBigNumber bigNumber(ByteBuffer input, AtomicInteger offset) {
        int start = offset.get();
        StringBuilder builder = new StringBuilder();
        offset.incrementAndGet();
        while (offset.get() + 1 < input.remaining() && input.get(offset.get() + 1) != '\n'
               && input.get(offset.get()) != '\r') {
            if (input.get(offset.get()) < '0' || input.get(offset.get()) > '9') {
                throw new IllegalArgumentException("Invalid big number format");
            }
            builder.append((char) input.get(offset.get()));
            offset.incrementAndGet();
        }
        offset.addAndGet(2);
        String stringValue = builder.toString();
        return new RespBigNumber(new BigDecimal(stringValue), offset.get() - start);
    }

    private RespValue respDouble(byte[] input, AtomicInteger offset) {
        int start = offset.get();
        StringBuilder builder = new StringBuilder();
        offset.incrementAndGet();
        while (offset.get() + 1 < input.length && input[offset.get() + 1] != '\n'
               && input[offset.get()] != '\r') {
            builder.append((char) input[offset.get()]);
            offset.incrementAndGet();
        }
        offset.addAndGet(2);
        String stringValue = builder.toString();
        int size = offset.get() - start;
        return switch (stringValue) {
            case "nan" -> new RespDouble(Double.NaN, size);
            case "inf" -> new RespDouble(Double.POSITIVE_INFINITY, size);
            case "-inf" -> new RespDouble(Double.NEGATIVE_INFINITY, size);
            default -> new RespDouble(Double.parseDouble(stringValue), size);
        };
    }

    private RespValue respDouble(ByteBuffer input, AtomicInteger offset) {
        int start = offset.get();
        StringBuilder builder = new StringBuilder();
        offset.incrementAndGet();
        while (offset.get() + 1 < input.remaining() && input.get(offset.get() + 1) != '\n'
               && input.get(offset.get()) != '\r') {
            builder.append((char) input.get(offset.get()));
            offset.incrementAndGet();
        }
        offset.addAndGet(2);
        String stringValue = builder.toString();
        int size = offset.get() - start;
        return switch (stringValue) {
            case "nan" -> new RespDouble(Double.NaN, size);
            case "inf" -> new RespDouble(Double.POSITIVE_INFINITY, size);
            case "-inf" -> new RespDouble(Double.NEGATIVE_INFINITY, size);
            default -> new RespDouble(Double.parseDouble(stringValue), size);
        };
    }

    private RespBoolean respBoolean(byte[] input, AtomicInteger offset) {
        offset.incrementAndGet();
        if (offset.get() >= input.length) {
            throw new IllegalArgumentException("Expected boolean value");
        }
        if (input[offset.get()] != 't' && input[offset.get()] != 'f') {
            throw new IllegalArgumentException("Invalid boolean format");
        }
        boolean value = input[offset.get()] == 't';
        offset.incrementAndGet();
        if (input[offset.get()] != '\r' || input[offset.get() + 1] != '\n') {
            throw new IllegalArgumentException("Boolean value must end with CRLF");
        }
        offset.addAndGet(2);
        return new RespBoolean(value);
    }

    private RespBoolean respBoolean(ByteBuffer input, AtomicInteger offset) {
        offset.incrementAndGet();
        if (offset.get() >= input.remaining()) {
            throw new IllegalArgumentException("Expected boolean value");
        }
        if (input.get(offset.get()) != 't' && input.get(offset.get()) != 'f') {
            throw new IllegalArgumentException("Invalid boolean format");
        }
        boolean value = input.get(offset.get()) == 't';
        offset.incrementAndGet();
        if (input.get(offset.get()) != '\r' || input.get(offset.get() + 1) != '\n') {
            throw new IllegalArgumentException("Boolean value must end with CRLF");
        }
        offset.addAndGet(2);
        return new RespBoolean(value);
    }

    private RespNull respNull(byte[] input, AtomicInteger offset) {
        offset.incrementAndGet();
        if (offset.get() + 1 >= input.length || input[offset.get()] != '\r'
            || input[offset.get() + 1] != '\n') {
            throw new IllegalArgumentException("Null value must end with CRLF");
        }
        offset.addAndGet(2);
        return new RespNull();
    }

    private RespNull respNull(ByteBuffer input, AtomicInteger offset) {
        offset.incrementAndGet();
        if (offset.get() + 1 >= input.remaining() || input.get(offset.get()) != '\r'
            || input.get(offset.get() + 1) != '\n') {
            throw new IllegalArgumentException("Null value must end with CRLF");
        }
        offset.addAndGet(2);
        return new RespNull();
    }

    private RespArray array(byte[] input, AtomicInteger offset) {
        int start = offset.get();
        int length = 0;
        offset.incrementAndGet();
        while (offset.get() + 1 < input.length && input[offset.get() + 1] != '\n'
               && input[offset.get()] != '\r') {
            if (input[offset.get()] < '0' || input[offset.get()] > '9') {
                throw new IllegalArgumentException("Invalid array length");
            }
            length = length * 10 + (input[offset.get()] - '0');
            offset.incrementAndGet();
        }
        offset.addAndGet(2);

        List<RespValue> values = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            if (offset.get() >= input.length) {
                throw new IllegalArgumentException("Expected array entry");
            }
            RespValue value = parse(input, offset);
            values.add(value);
        }
        return new RespArray(values, offset.get() - start);
    }

    private RespArray array(ByteBuffer input, AtomicInteger offset) {
        int start = offset.get();
        int length = 0;
        offset.incrementAndGet();
        while (offset.get() + 1 < input.remaining() && input.get(offset.get() + 1) != '\n'
               && input.get(offset.get()) != '\r') {
            if (input.get(offset.get()) < '0' || input.get(offset.get()) > '9') {
                throw new IllegalArgumentException("Invalid array length");
            }
            length = length * 10 + (input.get(offset.get()) - '0');
            offset.incrementAndGet();
        }
        offset.addAndGet(2);

        List<RespValue> values = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            if (offset.get() >= input.remaining()) {
                throw new IllegalArgumentException("Expected array entry");
            }
            RespValue value = parse(input, offset);
            values.add(value);
        }
        return new RespArray(values, offset.get() - start);
    }

    private RespBulkString bulkString(byte[] input, AtomicInteger offset) {
        int start = offset.get();
        int length = 0;
        offset.incrementAndGet();
        if (offset.get() + 2 < input.length && input[offset.get()] == '-' && input[offset.get() + 1] == '1') {
            offset.addAndGet(4);
            return new RespBulkString(null);
        }
        while (offset.get() + 1 < input.length && input[offset.get() + 1] != '\n'
               && input[offset.get()] != '\r') {
            if (input[offset.get()] < '0' || input[offset.get()] > '9') {
                throw new IllegalArgumentException("Invalid bulk string length");
            }
            length = length * 10 + (input[offset.get()] - '0');
            offset.incrementAndGet();
        }
        offset.addAndGet(2);
        StringBuilder builder = new StringBuilder();
        for (int i = offset.get(); i < offset.get() + length; i++) {
            if (i >= input.length || input[i] == '\r' || input[i] == '\n') {
                throw new IllegalArgumentException("Unexpected end of input " + offset + " " + (char) input[offset.get()]);
            }
            builder.append((char) input[i]);
        }
        if (offset.get() + length >= input.length
            || input[offset.get() + length] != '\r'
            || input[offset.get() + length + 1] != '\n') {
            throw new IllegalArgumentException("Bulk string must end with CRLF");
        }
        offset.addAndGet(length + 2);
        return new RespBulkString(builder.toString(), offset.get() - start);
    }

    private RespBulkString bulkString(ByteBuffer input, AtomicInteger offset) {
        int start = offset.get();
        int length = 0;
        offset.incrementAndGet();
        if (offset.get() + 2 < input.remaining() && input.get(offset.get()) == '-' && input.get(offset.get() + 1) == '1') {
            offset.addAndGet(4);
            return new RespBulkString(null);
        }
        while (offset.get() + 1 < input.remaining() && input.get(offset.get() + 1) != '\n'
               && input.get(offset.get()) != '\r') {
            if (input.get(offset.get()) < '0' || input.get(offset.get()) > '9') {
                throw new IllegalArgumentException("Invalid bulk string length");
            }
            length = length * 10 + (input.get(offset.get()) - '0');
            offset.incrementAndGet();
        }
        offset.addAndGet(2);
        StringBuilder builder = new StringBuilder();
        for (int i = offset.get(); i < offset.get() + length; i++) {
            if (i >= input.remaining() || input.get(i) == '\r' || input.get(i) == '\n') {
                throw new IllegalArgumentException("Unexpected end of input " + offset + " " + input.get(offset.get()));
            }
            builder.append((char) input.get(i));
        }
        if (offset.get() + length >= input.remaining()
            || input.get(offset.get() + length) != '\r'
            || input.get(offset.get() + length + 1) != '\n') {
            throw new IllegalArgumentException("Bulk string must end with CRLF");
        }
        offset.addAndGet(length + 2);
        return new RespBulkString(builder.toString(), offset.get() - start);
    }

    private RespInteger integer(byte[] input, AtomicInteger offset) {
        int start = offset.get();
        long value = 0;
        offset.incrementAndGet();
        while (offset.get() + 1 < input.length && input[offset.get() + 1] != '\n'
               && input[offset.get()] != '\r') {
            if (input[offset.get()] < '0' || input[offset.get()] > '9') {
                throw new IllegalArgumentException("Invalid number format");
            }
            value = value * 10 + (input[offset.get()] - '0');
            offset.incrementAndGet();
        }
        offset.addAndGet(2);
        return new RespInteger(value, offset.get() - start);
    }

    private RespInteger integer(ByteBuffer input, AtomicInteger offset) {
        int start = offset.get();
        long value = 0;
        offset.incrementAndGet();
        while (offset.get() + 1 < input.remaining() && input.get(offset.get() + 1) != '\n'
               && input.get(offset.get()) != '\r') {
            if (input.get(offset.get()) < '0' || input.get(offset.get()) > '9') {
                throw new IllegalArgumentException("Invalid number format");
            }
            value = value * 10 + (input.get(offset.get()) - '0');
            offset.incrementAndGet();
        }
        offset.addAndGet(2);
        return new RespInteger(value, offset.get() - start);
    }

    private RespError error(byte[] input, AtomicInteger offset) {
        int start = offset.get();
        StringBuilder builder = new StringBuilder();
        offset.incrementAndGet();
        while (offset.get() + 1 < input.length && input[offset.get() + 1] != '\n'
               && input[offset.get()] != '\r') {
            builder.append((char) input[offset.get()]);
            offset.incrementAndGet();
        }
        offset.addAndGet(2);
        return new RespError(builder.toString(), offset.get() - start);
    }

    private RespValue error(ByteBuffer input, AtomicInteger offset) {
        int start = offset.get();
        StringBuilder builder = new StringBuilder();
        offset.incrementAndGet();
        while (offset.get() + 1 < input.remaining() && input.get(offset.get() + 1) != '\n'
               && input.get(offset.get()) != '\r') {
            builder.append((char) input.get(offset.get()));
            offset.incrementAndGet();
        }
        offset.addAndGet(2);
        return new RespError(builder.toString(), offset.get() - start);
    }

    private RespSimpleString simpleString(byte[] input, AtomicInteger offset) {
        int start = offset.get();
        StringBuilder builder = new StringBuilder();
        offset.incrementAndGet();
        while (offset.get() < input.length && input[offset.get()] != '\r') {
            builder.append((char) input[offset.get()]);
            offset.incrementAndGet();
        }
        offset.addAndGet(2);
        return new RespSimpleString(builder.toString(), offset.get() - start);
    }

    private RespValue simpleString(ByteBuffer input, AtomicInteger offset) {
        int start = offset.get();
        StringBuilder builder = new StringBuilder();
        offset.incrementAndGet();
        while (offset.get() < input.remaining() && input.get(offset.get()) != '\r') {
            builder.append((char) input.get(offset.get()));
            offset.incrementAndGet();
        }
        offset.addAndGet(2);
        return new RespSimpleString(builder.toString(), offset.get() - start);
    }
}
