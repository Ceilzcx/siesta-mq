package com.ceilzcx.siestamq.remoting.protocol.header.nameserver;

import com.ceilzcx.siestamq.remoting.CommandCustomHeader;
import com.ceilzcx.siestamq.remoting.exception.RemotingCommandException;

/**
 * @author ceilzcx
 * @since 16/8/2023
 */
public class GetRouteDataRequestHeader implements CommandCustomHeader {
    private String topic;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
