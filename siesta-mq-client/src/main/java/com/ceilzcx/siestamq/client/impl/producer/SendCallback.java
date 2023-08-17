package com.ceilzcx.siestamq.client.impl.producer;

/**
 * @author ceilzcx
 * @since 3/4/2023
 */
public interface SendCallback {

    void onSuccess(final Object sendResult);

    void onException(final Throwable e);

}
