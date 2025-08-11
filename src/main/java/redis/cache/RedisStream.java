package redis.cache;

import redis.resp.RespBulkString;
import redis.resp.RespError;
import redis.resp.RespInteger;
import redis.resp.RespValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisStream {
    private static final RespError KEY_VALIDATION_ERROR
            = new RespError("ERR The ID specified in XADD is equal or smaller than the target stream top item");
    private static final RespError ZERO_KEY_VALIDATION_ERROR
            = new RespError("ERR The ID specified in XADD must be greater than 0-0");
    private final Trie trie;
    private String minEntryId;
    private String maxEntryId;

    public RedisStream() {
        this.trie = new Trie();
    }

    public RespValue append(RespBulkString entryId, List<RespValue> values) {
        String[] id = entryId.value().split("-");
        if (Long.parseLong(id[0]) == 0 && Long.parseLong(id[1]) == 0) {
            return ZERO_KEY_VALIDATION_ERROR;
        }
        if (minEntryId == null) {
            minEntryId = id[0];
            maxEntryId = id[0];
        } else if (maxEntryId.compareTo(id[0]) > 0) {
            return KEY_VALIDATION_ERROR;
        } else {
            maxEntryId = id[0];
        }

        return trie.insert(id, values);
    }

    private class Trie {
        private final TrieNode root;

        private Trie() {
            this.root = new TrieNode();
        }

        public RespValue insert(String[] id, List<RespValue> values) {
            TrieNode node = root;
            for (char c: id[0].toCharArray()) {
                if (!node.contains(c)) {
                    node.put(c);
                }
                node = node.get(c);
            }

            RespValue key = node.appendValue(id[1], values);
            if (key instanceof RespInteger intKey) {
                return new RespBulkString(id[0] + "-" + intKey.value());
            }

            return KEY_VALIDATION_ERROR;
        }
    }

    private class TrieNode {
        private final Map<Character, TrieNode> children;
        private final List<List<RespValue>> entries;
        private final List<Integer> ids;

        private TrieNode() {
            this.children = new HashMap<>();
            this.entries = new ArrayList<>();
            this.ids = new ArrayList<>();
        }

        public boolean contains(char c) {
            return children.containsKey(c);
        }

        public void put(char c) {
            children.put(c, new TrieNode());
        }

        public TrieNode get(char c) {
            return children.get(c);
        }

        public RespValue appendValue(String id, List<RespValue> values) {
            int entryId = Integer.parseInt(id);
            if (!ids.isEmpty() && entryId <= ids.getLast()) {
                return KEY_VALIDATION_ERROR;
            }

            entries.add(values);
            if (ids.isEmpty() || entryId > ids.getLast()) {
                ids.add(entryId);
                return new RespInteger(entryId);
            }

            ids.add(ids.getLast() + 1);
            return new RespInteger(ids.getLast());
        }
    }
}
