package com.ceilzcx.siestamq.client.common;

import java.util.Random;

/**
 * @author ceilzcx
 * @since 3/4/2023
 */
public class ThreadLocalIndex {
    private final ThreadLocal<Integer> threadLocalIndex = new ThreadLocal<>();
    private final Random random = new Random();
    private final static int POSITIVE_MASK = 0x7FFFFFFF;

    public int incrementAndGet() {
        Integer index = this.threadLocalIndex.get();
        if (null == index) {
            index = random.nextInt();
        }
        this.threadLocalIndex.set(++index);
        return Math.abs(index & POSITIVE_MASK);
    }

    @Override
    public String toString() {
        return "ThreadLocalIndex{" +
                "threadLocalIndex=" + threadLocalIndex.get() +
                '}';
    }
}
