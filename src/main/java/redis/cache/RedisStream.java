package redis.cache;

import redis.resp.RespBulkString;
import redis.resp.RespError;
import redis.resp.RespInteger;
import redis.resp.RespValue;

import java.util.*;

public class RedisStream {
    private static final RespError KEY_VALIDATION_ERROR
            = new RespError("ERR The ID specified in XADD is equal or smaller than the target stream top item");
    private static final RespError ZERO_KEY_VALIDATION_ERROR
            = new RespError("ERR The ID specified in XADD must be greater than 0-0");
    private final Trie trie;
    private long minEntryId;
    private long maxEntryId;

    public RedisStream() {
        trie = new Trie();
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

        return trie.insert(timePart, sequenceNumber, values);
    }

    private static class Trie {
        private final TrieNode root;

        private Trie() {
            this.root = new TrieNode();
        }

        public RespValue insert(long timePart, long sequenceNumber, List<RespValue> values) {
            TrieNode node = root;
            Deque<Integer> digits = getDigits(timePart);
            while (!digits.isEmpty()) {
                int digit = digits.pollLast();
                if (!node.contains(digit)) {
                    node.put(digit);
                }
                node = node.get(digit);
            }
            RespValue key = node.appendValue(timePart, sequenceNumber, values);
            if (key instanceof RespInteger intKey) {
                return new RespBulkString(timePart + "-" + intKey.value());
            }

            return KEY_VALIDATION_ERROR;
        }

        private Deque<Integer> getDigits(long number) {
            Deque<Integer> digits = new LinkedList<>();
            while (number > 0) {
                digits.push((int) (number % 10));
                number /= 10;
            }
            return digits;
        }
    }

    private static class TrieNode {
        private final TrieNode[] children;
        private final List<List<RespValue>> entries;
        private final List<Long> ids;

        private TrieNode() {
            this.children = new TrieNode[10];
            this.entries = new ArrayList<>();
            this.ids = new ArrayList<>();
        }

        public boolean contains(int digit) {
            return children[digit] != null;
        }

        public void put(int digit) {
            children[digit] = new TrieNode();
        }

        public TrieNode get(int digit) {
            return children[digit];
        }
        public RespValue appendValue(long timePart, long predefinedSequenceNumber, List<RespValue> values) {
            long sequenceNumber = getSequenceNumber(timePart, predefinedSequenceNumber);
            if (!ids.isEmpty() && sequenceNumber <= ids.getLast()) {
                return KEY_VALIDATION_ERROR;
            }

            entries.add(values);
            if (ids.isEmpty() || sequenceNumber > ids.getLast()) {
                ids.add(sequenceNumber);
                return new RespInteger(sequenceNumber);
            }

            ids.add(sequenceNumber);
            return new RespInteger(ids.getLast());
        }

        private long getSequenceNumber(long timePart, long predefinedSequenceNumber) {
            long entryId;
            if (predefinedSequenceNumber == -1) {
                if (ids.isEmpty()) {
                    if (timePart == 0) {
                        entryId = 1;
                    } else {
                        entryId = 0;
                    }
                } else {
                    entryId = ids.getLast() + 1;
                }
            } else {
                entryId = predefinedSequenceNumber;
            }
            return entryId;
        }
    }
}
