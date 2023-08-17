package com.ceilzcx.siestamq.remoting.protocol;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author ceilzcx
 * @since 20/12/2022
 */
public class DataVersion extends RemotingSerializable {
    private long stateVersion = 0L;
    private long timestamp = System.currentTimeMillis();
    private AtomicLong counter = new AtomicLong(0);

    public void assignNewOne(final DataVersion dataVersion) {
        this.timestamp = dataVersion.timestamp;
        this.stateVersion = dataVersion.stateVersion;
        this.counter.set(dataVersion.counter.get());
    }

    public void nextVersion() {
        this.nextVersion(0L);
    }

    public void nextVersion(long stateVersion) {
        this.timestamp = System.currentTimeMillis();
        this.stateVersion = stateVersion;
        this.counter.incrementAndGet();
    }

    public long getStateVersion() {
        return stateVersion;
    }

    public void setStateVersion(long stateVersion) {
        this.stateVersion = stateVersion;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public AtomicLong getCounter() {
        return counter;
    }

    public void setCounter(AtomicLong counter) {
        this.counter = counter;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DataVersion version = (DataVersion) o;

        if (getStateVersion() != version.getStateVersion())
            return false;
        if (getTimestamp() != version.getTimestamp())
            return false;

        if (counter != null && version.counter != null) {
            return counter.longValue() == version.counter.longValue();
        }

        return null == counter && null == version.counter;

    }

    @Override
    public int hashCode() {
        int result = (int) (getStateVersion() ^ (getStateVersion() >>> 32));
        result = 31 * result + (int) (getTimestamp() ^ (getTimestamp() >>> 32));
        if (null != counter) {
            long l = counter.get();
            result = 31 * result + (int) (l ^ (l >>> 32));
        }
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DataVersion[");
        sb.append("timestamp=").append(timestamp);
        sb.append(", counter=").append(counter);
        sb.append(']');
        return sb.toString();
    }

    public int compare(DataVersion dataVersion) {
        if (this.getStateVersion() > dataVersion.getStateVersion()) {
            return 1;
        } else if (this.getStateVersion() < dataVersion.getStateVersion()) {
            return -1;
        } else if (this.getCounter().get() > dataVersion.getCounter().get()) {
            return 1;
        } else if (this.getCounter().get() < dataVersion.getCounter().get()) {
            return -1;
        } else if (this.getTimestamp() > dataVersion.getTimestamp()) {
            return 1;
        } else if (this.getTimestamp() < dataVersion.getTimestamp()) {
            return -1;
        }
        return 0;
    }
}
