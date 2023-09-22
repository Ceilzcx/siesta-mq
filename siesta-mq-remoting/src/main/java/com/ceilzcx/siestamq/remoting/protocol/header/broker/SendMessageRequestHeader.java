package com.ceilzcx.siestamq.remoting.protocol.header.broker;

import com.ceilzcx.siestamq.remoting.exception.RemotingCommandException;
import com.ceilzcx.siestamq.remoting.rpc.TopicQueueRequestHeader;

/**
 * @author ceilzcx
 * @since 21/9/2023
 * 发现消息的header
 */
public class SendMessageRequestHeader extends TopicQueueRequestHeader {

    private String producerGroup;

    private String topic;

    // todo
    private String defaultTopic;

    // todo

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    @Override
    public String getTopic() {
        return null;
    }

    @Override
    public void setTopic(String topic) {

    }

    @Override
    public void checkFields() throws RemotingCommandException {

    }
}
