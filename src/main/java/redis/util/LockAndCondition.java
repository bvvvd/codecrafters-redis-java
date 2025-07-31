package redis.util;

import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public final class LockAndCondition {
    private final Lock lock;
    private final Condition condition;
    private volatile boolean passed;

    public LockAndCondition(Lock lock,
                            Condition condition) {
        this.lock = lock;
        this.condition = condition;
    }

    public Lock getLock() {
        return lock;
    }

    public Condition getCondition() {
        return condition;
    }

    public boolean passed() {
        return passed;
    }

    public void pass() {
        this.passed = true;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (LockAndCondition) obj;
        return Objects.equals(this.lock, that.lock) &&
               Objects.equals(this.condition, that.condition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lock, condition);
    }

    @Override
    public String toString() {
        return "LockAndCondition[" +
               "lock=" + lock + ", " +
               "condition=" + condition + ']';
    }
}
