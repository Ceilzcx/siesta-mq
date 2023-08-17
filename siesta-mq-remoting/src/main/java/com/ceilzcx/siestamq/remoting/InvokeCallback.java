package com.ceilzcx.siestamq.remoting;

import com.ceilzcx.siestamq.remoting.netty.ResponseFuture;

/**
 * @author ceilzcx
 * @since 8/12/2022
 * 异步回调接口
 */
public interface InvokeCallback {

    void operationComplete(final ResponseFuture responseFuture);
}
