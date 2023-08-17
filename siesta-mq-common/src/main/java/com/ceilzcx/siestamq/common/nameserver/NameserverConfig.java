package com.ceilzcx.siestamq.common.nameserver;

/**
 * @author ceilzcx
 * @since 15/12/2022
 */
public class NameserverConfig {

    private int defaultThreadPoolNums = 16;

    private int defaultThreadPoolQueueCapacity = 10000;

    public int getDefaultThreadPoolNums() {
        return defaultThreadPoolNums;
    }

    public void setDefaultThreadPoolNums(int defaultThreadPoolNums) {
        this.defaultThreadPoolNums = defaultThreadPoolNums;
    }

    public int getDefaultThreadPoolQueueCapacity() {
        return defaultThreadPoolQueueCapacity;
    }

    public void setDefaultThreadPoolQueueCapacity(int defaultThreadPoolQueueCapacity) {
        this.defaultThreadPoolQueueCapacity = defaultThreadPoolQueueCapacity;
    }
}
