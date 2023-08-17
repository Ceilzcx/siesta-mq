package com.ceilzcx.siestamq.client.producer;

import com.ceilzcx.siestamq.client.common.ThreadLocalIndex;
import com.ceilzcx.siestamq.common.message.MessageQueue;
import com.ceilzcx.siestamq.remoting.protocol.route.QueueData;
import com.ceilzcx.siestamq.remoting.protocol.route.TopicRouteData;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ceilzcx
 * @since 3/4/2023
 */
public class TopicPublishInfo {

    private List<MessageQueue> messageQueueList = new ArrayList<>();
    private TopicRouteData topicRouteData;

    private volatile ThreadLocalIndex sendWithQueue = new ThreadLocalIndex();

    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }

    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public void setTopicRouteData(TopicRouteData topicRouteData) {
        this.topicRouteData = topicRouteData;
    }

    public ThreadLocalIndex getSendWithQueue() {
        return sendWithQueue;
    }

    public boolean ok() {
        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }

    public MessageQueue selectOneMessageQueue(String lastBrokerName) {
        if (null == lastBrokerName) {
            return this.selectOneMessageQueue();
        }
        for (int i = 0; i < messageQueueList.size(); i++) {
            MessageQueue messageQueue = this.selectOneMessageQueue();
            if (!lastBrokerName.equals(messageQueue.getBrokerName())) {
                return messageQueue;
            }
        }
        return this.selectOneMessageQueue();
    }

    public MessageQueue selectOneMessageQueue() {
        int index = this.sendWithQueue.incrementAndGet();
        int pos = index % this.messageQueueList.size();
        return this.messageQueueList.get(pos);
    }

    public int getWriteQueueNumsByBrokerName(final String brokerName) {
        if (brokerName == null) {
            return -1;
        }
        for (QueueData queueData : topicRouteData.getQueueDatas()) {
            if (brokerName.equals(queueData.getBrokerName())) {
                return queueData.getWriteQueueNums();
            }
        }
        return -1;
    }
}
