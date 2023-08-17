package com.ceilzcx.siestamq.common.message;

import java.io.Serializable;

/**
 * @author ceilzcx
 * @since 23/12/2022
 */
public class Message implements Serializable {
    private String topic;
    private String transactionId;
    private int flag;
    private byte[] body;

    public Message() {
    }

    public Message(String topic, byte[] body) {
        this.topic = topic;
        this.body = body;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }
}
