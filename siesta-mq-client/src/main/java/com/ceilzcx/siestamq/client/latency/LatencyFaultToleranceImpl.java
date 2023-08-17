package com.ceilzcx.siestamq.client.latency;

import com.ceilzcx.siestamq.client.common.ThreadLocalIndex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author ceilzcx
 * @since 4/4/2023
 */
public class LatencyFaultToleranceImpl implements LatencyFaultTolerance<String> {
    private final ConcurrentHashMap<String, FaultItem> faultItemTable = new ConcurrentHashMap<>(16);
    private final ThreadLocalIndex randomItem = new ThreadLocalIndex();

    @Override
    public void updateFaultItem(String name, long currentLatency, long notAvailableDuration) {
        FaultItem faultItem = this.faultItemTable.get(name);
        if (null == faultItem) {
            faultItem = new FaultItem(name);
            this.faultItemTable.putIfAbsent(name, faultItem);
        }
        faultItem.setCurrentLatency(currentLatency);
        faultItem.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);
    }

    @Override
    public boolean isAvailable(String name) {
        FaultItem faultItem = this.faultItemTable.get(name);
        if (faultItem != null) {
            return faultItem.isAvailable();
        }
        return true;
    }

    @Override
    public void remove(String name) {
        this.faultItemTable.remove(name);
    }

    /**
     * 如果所有的broker都曾被标记过故障怎么办?
     * 排序后, 从延迟小的一半中选一个broker
     * @return brokerName
     */
    @Override
    public String pickOneAtLeast() {
        final Enumeration<FaultItem> elements = this.faultItemTable.elements();
        List<FaultItem> tmpList = new ArrayList<>();
        while (elements.hasMoreElements()) {
            FaultItem faultItem = elements.nextElement();
            tmpList.add(faultItem);
        }
        if (!tmpList.isEmpty()) {
            Collections.sort(tmpList);
            int half = tmpList.size() / 2;
            if (half <= 0) {
                return tmpList.get(0).getName();
            } else {
                int i = this.randomItem.incrementAndGet() % half;
                return tmpList.get(i).getName();
            }
        }
        return null;
    }

    static class FaultItem implements Comparable<FaultItem> {
        /**
         * brokerName?
         */
        private final String name;

        /**
         * 实际的延迟时间(消息的消费时间)
         */
        private volatile long currentLatency;

        /**
         * 延迟后的启动时间
         */
        private volatile long startTimestamp;

        public FaultItem(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setCurrentLatency(long currentLatency) {
            this.currentLatency = currentLatency;
        }

        public long getCurrentLatency() {
            return currentLatency;
        }

        public void setStartTimestamp(long startTimestamp) {
            this.startTimestamp = startTimestamp;
        }

        public long getStartTimestamp() {
            return startTimestamp;
        }

        public boolean isAvailable() {
            return System.currentTimeMillis() >= startTimestamp;
        }

        @Override
        public int compareTo(final FaultItem other) {
            if (this.isAvailable() != other.isAvailable()) {
                if (this.isAvailable())
                    return -1;

                if (other.isAvailable())
                    return 1;
            }

            if (this.currentLatency < other.currentLatency)
                return -1;
            else if (this.currentLatency > other.currentLatency) {
                return 1;
            }

            if (this.startTimestamp < other.startTimestamp)
                return -1;
            else if (this.startTimestamp > other.startTimestamp) {
                return 1;
            }

            return 0;
        }

        @Override
        public int hashCode() {
            int result = getName() != null ? getName().hashCode() : 0;
            result = 31 * result + (int) (getCurrentLatency() ^ (getCurrentLatency() >>> 32));
            result = 31 * result + (int) (getStartTimestamp() ^ (getStartTimestamp() >>> 32));
            return result;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof FaultItem))
                return false;

            final FaultItem faultItem = (FaultItem) o;

            if (getCurrentLatency() != faultItem.getCurrentLatency())
                return false;
            if (getStartTimestamp() != faultItem.getStartTimestamp())
                return false;
            return getName() != null ? getName().equals(faultItem.getName()) : faultItem.getName() == null;

        }

        @Override
        public String toString() {
            return "FaultItem{" +
                    "name='" + name + '\'' +
                    ", currentLatency=" + currentLatency +
                    ", startTimestamp=" + startTimestamp +
                    '}';
        }
    }
}
