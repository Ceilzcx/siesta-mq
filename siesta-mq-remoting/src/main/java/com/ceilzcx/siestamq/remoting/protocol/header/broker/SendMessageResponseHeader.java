package com.ceilzcx.siestamq.remoting.protocol.header.broker;

import com.ceilzcx.siestamq.remoting.exception.RemotingCommandException;
import com.ceilzcx.siestamq.remoting.rpc.TopicQueueRequestHeader;

/**
 * @author ceilzcx
 * @since 28/9/2023
 */
public class SendMessageResponseHeader extends TopicQueueRequestHeader {

    private String msgId;

    private Integer queueId;

    private Long queueOffset;

    private String transactionId;

    private String batchUniqId;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    @Override
    public String getTopic() {
        return null;
    }

    @Override
    public void setTopic(String topic) {

    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public Long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(Long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }
}
