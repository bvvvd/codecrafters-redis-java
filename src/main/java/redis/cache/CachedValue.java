package redis.cache;

import redis.exception.CacheValueExpiredException;

import static redis.util.Logger.debug;

public record CachedValue<T>(T value, long expirationTime) {

    /**
     * Checks if the cached value is still valid based on the current time.
     *
     * @return true if the cached value is still valid, false otherwise
     */
    public boolean isValid() {
        long currentTime = System.currentTimeMillis();
        debug("Checking validity of cached value: %s, current time: %s, expiration time: %s",
                value, currentTime, expirationTime);
        return expirationTime == -1 || currentTime <= expirationTime;
    }

    /**
     * Gets the value of the cached item.
     *
     * @return the cached value
     */
    public T getValue() {
        if (!isValid()) {
            throw new CacheValueExpiredException("Cached value has expired.");
        }
        return value;
    }
}
