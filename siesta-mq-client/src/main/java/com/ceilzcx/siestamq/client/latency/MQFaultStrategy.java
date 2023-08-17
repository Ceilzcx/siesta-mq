package com.ceilzcx.siestamq.client.latency;

import com.ceilzcx.siestamq.client.producer.TopicPublishInfo;
import com.ceilzcx.siestamq.common.message.MessageQueue;

/**
 * @author ceilzcx
 * @since 4/4/2023
 */
public class MQFaultStrategy {

    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    private boolean sendLatencyFaultEnable = false;

    /**
     * 实际的消息消费的延迟时间max
     */
    private final long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};

    /**
     * 通过延迟时间评估不可用的时间段区间<br/>
     * todo 不知道这个时间段的原理
     */
    private final long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    public MessageQueue selectOneMessageQueue(final TopicPublishInfo topicPublishInfo, final String lastBrokerName) {
        if (this.isSendLatencyFaultEnable()) {
            int index = topicPublishInfo.getSendWithQueue().incrementAndGet();
            for (int i = 0; i < topicPublishInfo.getMessageQueueList().size(); i++) {
                int pos = index++ % topicPublishInfo.getMessageQueueList().size();
                MessageQueue messageQueue = topicPublishInfo.getMessageQueueList().get(pos);
                if (!lastBrokerName.equals(messageQueue.getBrokerName()) && latencyFaultTolerance.isAvailable(messageQueue.getBrokerName())) {
                    return messageQueue;
                }
            }

            final String notBestBrokerName = latencyFaultTolerance.pickOneAtLeast();
            int writeQueueNums = topicPublishInfo.getWriteQueueNumsByBrokerName(notBestBrokerName);
            if (writeQueueNums > 0) {
                MessageQueue messageQueue = topicPublishInfo.selectOneMessageQueue();
                // todo 为什么要怎么做
                if (null != messageQueue) {
                    messageQueue.setBrokerName(notBestBrokerName);
                    messageQueue.setQueueId(topicPublishInfo.getSendWithQueue().incrementAndGet() & writeQueueNums);
                }
            } else {
                latencyFaultTolerance.remove(notBestBrokerName);
            }
        }
        return topicPublishInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     * update失败的broker信息
     * @param brokerName broker name
     * @param currentLatency 当前延迟时间
     * @param isolation todo 需要隔离? 单独隔离这个broker, 所以默认用最大的不可用时间
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.isSendLatencyFaultEnable()) {
            long notAvailableDuration = this.computeNotAvailableDuration(isolation ? 30000: currentLatency);
            latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, notAvailableDuration);
        }
    }

    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = this.latencyMax.length - 1; i >= 0; i--) {
            if (this.latencyMax[i] <= currentLatency) {
                return this.notAvailableDuration[i];
            }
        }
        return 0;
    }
}
