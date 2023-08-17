package com.ceilzcx.siestamq.remoting.protocol.header.broker;

import com.ceilzcx.siestamq.remoting.CommandCustomHeader;
import com.ceilzcx.siestamq.remoting.exception.RemotingCommandException;

/**
 * @author hujiaofen
 * @since 16/12/2022
 */
public class CreateTopicRequestHeader implements CommandCustomHeader {
    private String topic;
    // 读的queue数量
    private Integer readQueueNums;

    private Integer writeQueueNums;
    // topic权限
    private Integer perm;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getReadQueueNums() {
        return readQueueNums;
    }

    public void setReadQueueNums(Integer readQueueNums) {
        this.readQueueNums = readQueueNums;
    }

    public Integer getWriteQueueNums() {
        return writeQueueNums;
    }

    public void setWriteQueueNums(Integer writeQueueNums) {
        this.writeQueueNums = writeQueueNums;
    }

    public Integer getPerm() {
        return perm;
    }

    public void setPerm(Integer perm) {
        this.perm = perm;
    }
}
