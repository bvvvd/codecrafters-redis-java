package redis.cache;

import redis.resp.RespValue;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class RedisSortedSet {
    private final PriorityQueue<ScoredValue> priority;
    private final Map<Double, RespValue> scoreToValueMap;
    private final Map<RespValue, Double> valueToScoreMap;

    public RedisSortedSet() {
        priority = new PriorityQueue<>(Comparator.comparingDouble(ScoredValue::score));
        scoreToValueMap = new HashMap<>();
        valueToScoreMap = new HashMap<>();
    }

    public boolean add(RespValue value, double score) {
        ScoredValue scoredValue = new ScoredValue(score, value);
        if (valueToScoreMap.containsKey(value)) {
            Double oldScore = valueToScoreMap.get(value);
            ScoredValue oldScoredValue = new ScoredValue(oldScore, value);
            priority.remove(oldScoredValue);
            scoreToValueMap.remove(oldScore);
            scoreToValueMap.put(score, value);
            valueToScoreMap.put(value, score);
            priority.add(scoredValue);
            return false;
        } else {
            priority.add(scoredValue);
            scoreToValueMap.put(score, value);
            valueToScoreMap.put(value, score);
            return true;
        }
    }

    private record ScoredValue(double score, RespValue value) {

    }
}
