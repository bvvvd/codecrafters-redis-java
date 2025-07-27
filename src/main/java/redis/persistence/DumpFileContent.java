package redis.persistence;

import redis.cache.CachedValue;

import java.util.Map;
import java.util.Set;

public final class DumpFileContent {
    private final Map<String, CachedValue<String>> content;

    DumpFileContent(Map<String, CachedValue<String>> content) {
        this.content = content;
    }

    public Set<String> keys() {
        return content.keySet();
    }

    public CachedValue<String> get(String key) {
        CachedValue<String> cachedValue = content.get(key);
        if (cachedValue == null) {
            return null;
        }
        return cachedValue.isValid() ? cachedValue : null;
    }

    @Override
    public String toString() {
        return "DumpFileContent{" +
               "content=" + content +
               '}';
    }
}
