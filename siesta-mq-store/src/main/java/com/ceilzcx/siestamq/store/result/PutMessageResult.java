package com.ceilzcx.siestamq.store.result;

/**
 * @author ceilzcx
 * @since 23/12/2022
 */
public class PutMessageResult {
    private PutMessageStatus putMessageStatus;

    public PutMessageStatus getPutMessageStatus() {
        return putMessageStatus;
    }

    public void setPutMessageStatus(PutMessageStatus putMessageStatus) {
        this.putMessageStatus = putMessageStatus;
    }
}
