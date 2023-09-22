package com.ceilzcx.siestamq.remoting.rpc;

import com.ceilzcx.siestamq.remoting.CommandCustomHeader;

/**
 * @author ceilzcx
 * @since 21/9/2023
 */
public abstract class TopicRequestHeader implements CommandCustomHeader {
    // todo static topic地方用到, 暂时不清楚含义
    private Boolean logical;

    public abstract String getTopic();

    public abstract void setTopic(String topic);

    public Boolean getLogical() {
        return logical;
    }

    public void setLogical(Boolean logical) {
        this.logical = logical;
    }
}
