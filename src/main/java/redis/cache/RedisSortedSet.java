package redis.cache;

import redis.resp.RespBulkString;
import redis.resp.RespValue;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

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

    private record ScoredValue(double score, RespValue value) {

    }
}
