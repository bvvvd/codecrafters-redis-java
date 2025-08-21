package redis.cache;

import redis.resp.RespArray;
import redis.resp.RespBulkString;
import redis.resp.RespInteger;
import redis.resp.RespValue;

import java.util.*;

public class RedisSortedSet {
    private final TreeSet<ScoredValue> scoreToValueMap;
    private final Map<RespValue, Double> valueToScoreMap;

    public RedisSortedSet() {
        scoreToValueMap
                = new TreeSet<>(Comparator.comparingDouble(ScoredValue::score)
                .thenComparing(scoredValue -> ((RespBulkString) scoredValue.value).value()));
        valueToScoreMap = new HashMap<>();
    }

    public boolean add(RespValue value, double score) {
        if (valueToScoreMap.containsKey(value)) {
            Double oldScore = valueToScoreMap.get(value);
            scoreToValueMap.remove(new ScoredValue(oldScore, value));
            scoreToValueMap.add(new ScoredValue(score, value));
            valueToScoreMap.put(value, score);
            return false;
        } else {
            scoreToValueMap.add(new ScoredValue(score, value));
            valueToScoreMap.put(value, score);
            return true;
        }
    }

    public long rank(RespValue value) {
        Double score = valueToScoreMap.get(value);
        if (score == null) {
            return -1;
        }

        ScoredValue scoredValue = new ScoredValue(score, value);
        return scoreToValueMap.headSet(scoredValue).size();
    }

    public RespArray range(int start, int end) {
        start = normalize(start);
        end = normalize(end);
        if (start > end) {
            return new RespArray(List.of());
        }
        return new RespArray(
                scoreToValueMap.stream()
                        .skip(start)
                        .limit(end - start + 1)
                        .map(ScoredValue::value).toList());
    }

    private int normalize(int index) {
        if (index < 0) {
            if (-index > scoreToValueMap.size()) {
                return 0;
            }
            return (index % scoreToValueMap.size()) + scoreToValueMap.size();
        }

        return index;
    }

    public RespValue size() {
        return new RespInteger(scoreToValueMap.size());
    }

    public RespValue score(RespValue value) {
        Double score = valueToScoreMap.get(value);
        return score == null ? new RespBulkString(null) : new RespBulkString(score.toString());
    }

    private record ScoredValue(double score, RespValue value) {

    }
}
