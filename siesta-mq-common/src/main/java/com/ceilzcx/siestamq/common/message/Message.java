package com.ceilzcx.siestamq.common.message;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author ceilzcx
 * @since 23/12/2022
 */
public class Message implements Serializable {
    private String topic;

    private String transactionId;

    private int flag;

    private Map<String, String> properties;

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

    void putProperty(final String name, final String value) {
        if (null == this.properties) {
            this.properties = new HashMap<>();
        }

        this.properties.put(name, value);
    }

    void clearProperty(final String name) {
        if (null != this.properties) {
            this.properties.remove(name);
        }
    }

    public String getProperty(final String name) {
        if (null == this.properties) {
            this.properties = new HashMap<>();
        }

        return this.properties.get(name);
    }

    public int getDelayTimeLevel() {
        String t = this.getProperty(MessageConstants.PROPERTY_DELAY_TIME_LEVEL);
        if (t != null) {
            return Integer.parseInt(t);
        }

        return 0;
    }
}
