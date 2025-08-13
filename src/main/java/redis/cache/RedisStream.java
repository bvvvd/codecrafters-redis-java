package redis.cache;

import redis.resp.RespArray;
import redis.resp.RespBulkString;
import redis.resp.RespError;
import redis.resp.RespValue;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import static redis.util.Logger.debug;

public class RedisStream {
    private static final RespError KEY_VALIDATION_ERROR
            = new RespError("ERR The ID specified in XADD is equal or smaller than the target stream top item");
    private static final RespError ZERO_KEY_VALIDATION_ERROR
            = new RespError("ERR The ID specified in XADD must be greater than 0-0");
    private final TreeMap<Long, StreamNode> streamStorage;
    private long minEntryId;
    private long maxEntryId;

    public RedisStream() {
        streamStorage = new TreeMap<>();
        minEntryId = -1;
        maxEntryId = -1;
    }

    public RespValue append(RespBulkString entryId, List<RespValue> values) {
        String value = entryId.value();
        long timePart = -1;
        long sequenceNumber = -1;
        if ("*".equalsIgnoreCase(value)) {
            timePart = System.currentTimeMillis();
        }
        String[] rawIds = value.split("-");
        if (timePart == -1) {
            timePart = Long.parseLong(rawIds[0]);
        }
        if (rawIds.length != 1 && !"*".equalsIgnoreCase(rawIds[1])) {
            sequenceNumber = Long.parseLong(rawIds[1]);
        }
        if (timePart == 0 && sequenceNumber == 0) {
            return ZERO_KEY_VALIDATION_ERROR;
        }
        if (minEntryId == -1) {
            minEntryId = timePart;
            maxEntryId = timePart;
        } else if (maxEntryId > timePart) {
            return KEY_VALIDATION_ERROR;
        } else {
            maxEntryId = timePart;
        }

        return streamStorage.computeIfAbsent(timePart, k -> new StreamNode())
                .append(timePart, sequenceNumber, values);
    }

    public RespValue range(String left, String right) {
        long[] start = getStart(left);
        long[] end = getEnd(right);

        List<RespValue> rangeValues = new ArrayList<>();
        streamStorage.subMap(start[0], true, end[0], true)
                .forEach((timePart, node) -> {
                    long from = node.values.firstKey();
                    long to = node.values.lastKey();
                    if (timePart == start[0]) {
                        from = start[1];
                    }
                    if (timePart == end[0]) {
                        to = end[1];
                    }

                    node.values.subMap(from, true, to, true)
                            .forEach((key, value)
                                    -> rangeValues.add(
                                    new RespArray(
                                            List.of(
                                                    new RespBulkString(timePart + "-" + key),
                                                    new RespArray(value)))));
                });
        return new RespArray(rangeValues);
    }

    private long[] getStart(String left) {
        if (left == null) {
            return new long[]{minEntryId, -1};
        }
        String[] split = left.split("-");
        if (split.length == 1) {
            return new long[]{Math.max(minEntryId, Long.parseLong(split[0])), -1};
        }

        return new long[]{Math.max(minEntryId, Long.parseLong(split[0])), Long.parseLong(split[1])};
    }

    private long[] getEnd(String right) {
        if (right == null) {
            return new long[]{maxEntryId, -1};
        }
        String[] split = right.split("-");
        if (split.length == 1) {
            return new long[]{Math.min(maxEntryId, Long.parseLong(split[0])), -1};
        }

        return new long[]{Math.min(maxEntryId, Long.parseLong(split[0])), Long.parseLong(split[1])};
    }

    private class StreamNode {
        TreeMap<Long, List<RespValue>> values = new TreeMap<>();

        public RespValue append(long timePart, long predefinedSequenceNumber, List<RespValue> streamValues) {
            long sequenceNumber = getSequenceNumber(timePart, predefinedSequenceNumber);
            if (!values.isEmpty() && sequenceNumber <= values.lastKey()) {
                return KEY_VALIDATION_ERROR;
            }

            values.put(sequenceNumber, streamValues);
            return new RespBulkString(timePart + "-" + sequenceNumber);
        }

        private long getSequenceNumber(long timePart, long predefinedSequenceNumber) {
            long entryId;
            if (predefinedSequenceNumber == -1) {
                if (values.isEmpty()) {
                    if (timePart == 0) {
                        entryId = 1;
                    } else {
                        entryId = 0;
                    }
                } else {
                    entryId = values.lastKey() + 1;
                }
            } else {
                entryId = predefinedSequenceNumber;
            }
            return entryId;
        }
    }
}
