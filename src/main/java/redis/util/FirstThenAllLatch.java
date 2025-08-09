package redis.util;

import redis.resp.RespValue;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static redis.util.Logger.debug;

public class FirstThenAllLatch {
    private final ReentrantLock lock = new ReentrantLock(true); // fair lock
    private final Condition firstThreadCondition = lock.newCondition();
    private final Condition remainingThreadsCondition = lock.newCondition();

    private volatile boolean firstReleased = false;
    private volatile boolean releaseCalled = false;
    private final AtomicInteger arrivalIndex = new AtomicInteger(0);

    public void await(Runnable guardedCode) throws InterruptedException {
        lock.lock();
        try {
            debug("thread " + Thread.currentThread().getName() + " acquired latch");
            int myIndex = arrivalIndex.getAndIncrement();

            // Wait for the release to be triggered
            while (!releaseCalled) {
                firstThreadCondition.await();
            }

            if (myIndex == 0) {
                // I am the first thread
                guardedCode.run();
                firstReleased = true;
                remainingThreadsCondition.signalAll(); // allow others to proceed
                return;
            }

            // Remaining threads wait until first is released
            while (!firstReleased) {
                remainingThreadsCondition.await();
            }
        } finally {
            debug("thread " + Thread.currentThread().getName() + " released latch");
            lock.unlock();
        }
    }

    public void release() {
        lock.lock();
        try {
            releaseCalled = true;
            firstThreadCondition.signalAll();  // Wake the first thread
        } finally {
            lock.unlock();
        }
    }

    public void reset() {
        lock.lock();
        try {
            firstReleased = false;
            releaseCalled = false;
            arrivalIndex.set(0);
        } finally {
            lock.unlock();
        }
    }
}
