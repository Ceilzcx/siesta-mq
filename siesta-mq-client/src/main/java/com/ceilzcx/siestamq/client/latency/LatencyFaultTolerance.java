package com.ceilzcx.siestamq.client.latency;

/**
 * @author ceilzcx
 * @since 4/4/2023
 */
public interface LatencyFaultTolerance<T> {

    void updateFaultItem(final T name, final long currentLatency, final long notAvailableDuration);

    boolean isAvailable(final T name);

    void remove(final T name);

    T pickOneAtLeast();
}
