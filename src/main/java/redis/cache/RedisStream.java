package redis.cache;

import redis.resp.RespBulkString;
import redis.resp.RespValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisStream {
    private final Trie trie;
    private String minEntryId;
    private String maxEntryId;

    public RedisStream() {
        this.trie = new Trie();
    }

    public RespBulkString append(RespBulkString entryId, List<RespValue> values) {
        String[] id = entryId.value().split("-");
        if (minEntryId == null) {
            minEntryId = id[0];
            maxEntryId = id[0];
        } else if (maxEntryId.compareTo(id[0]) > 0) {
            id[0] = Long.toString(Long.parseLong(id[0]));
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

        public RespBulkString insert(String[] id, List<RespValue> values) {
            TrieNode node = root;
            for (char c: id[0].toCharArray()) {
                if (!node.contains(c)) {
                    node.put(c);
                }
                node = node.get(c);
            }

            return new RespBulkString(id[0] + "-" + node.appendValue(id[1], values));
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

        public int appendValue(String id, List<RespValue> values) {
            int entryId = Integer.parseInt(id);
            entries.add(values);
            if (ids.isEmpty() || entryId > ids.getLast()) {
                ids.add(entryId);
                return entryId;
            }

            ids.add(ids.getLast() + 1);
            return ids.getLast();
        }
    }
}
