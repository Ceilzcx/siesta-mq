package com.ceilzcx.siestamq.client.hook;

import com.ceilzcx.siestamq.client.impl.producer.DefaultMQProducerImpl;
import com.ceilzcx.siestamq.client.producer.SendResult;
import com.ceilzcx.siestamq.common.message.Message;
import com.ceilzcx.siestamq.common.message.MessageQueue;

/**
 * @author ceilzcx
 * @since 27/9/2023
 * hook的上下文类
 */
public class SendMessageContext {

    private String producerGroup;

    private Message message;

    private MessageQueue messageQueue;

    private SendResult sendResult;

    private DefaultMQProducerImpl producer;

    private Exception exception;

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public void setMessageQueue(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    public SendResult getSendResult() {
        return sendResult;
    }

    public void setSendResult(SendResult sendResult) {
        this.sendResult = sendResult;
    }

    public DefaultMQProducerImpl getProducer() {
        return producer;
    }

    public void setProducer(DefaultMQProducerImpl producer) {
        this.producer = producer;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }
}
