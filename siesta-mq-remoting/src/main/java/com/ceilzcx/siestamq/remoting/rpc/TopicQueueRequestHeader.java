package com.ceilzcx.siestamq.remoting.rpc;

/**
 * @author ceilzcx
 * @since 21/9/2023
 */
public abstract class TopicQueueRequestHeader extends TopicRequestHeader {
    /**
     * 队列id
     */
    private Integer queueId;

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

}
