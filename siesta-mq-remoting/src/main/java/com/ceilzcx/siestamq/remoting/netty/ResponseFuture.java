package com.ceilzcx.siestamq.remoting.netty;

import com.ceilzcx.siestamq.remoting.InvokeCallback;
import com.ceilzcx.siestamq.remoting.common.SemaphoreReleaseOnlyOnce;
import com.ceilzcx.siestamq.remoting.protocol.RemotingCommand;
import io.netty.channel.Channel;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author ceilzcx
 * @since 8/12/2022
 */
public class ResponseFuture {
    private final Channel channel;
    // request id
    private final int opaque;
    private final RemotingCommand request;
    private final long timeoutMillis;
    private final InvokeCallback invokeCallback;
    private final SemaphoreReleaseOnlyOnce once;
    private final long beginTimestamp = System.currentTimeMillis();

    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    private final AtomicBoolean executeCallbackOnlyOnce = new AtomicBoolean(false);

    private volatile RemotingCommand response;
    private volatile boolean interrupted = false;
    private volatile boolean sendRequestOK;
    private volatile Throwable cause;

    public ResponseFuture(Channel channel, int opaque, long timeoutMillis,
                          InvokeCallback invokeCallback, SemaphoreReleaseOnlyOnce once) {
        this(channel, opaque, null, timeoutMillis, invokeCallback, once);
    }

    public ResponseFuture(Channel channel, int opaque, RemotingCommand request, long timeoutMillis,
                          InvokeCallback invokeCallback, SemaphoreReleaseOnlyOnce once) {
        this.channel = channel;
        this.opaque = opaque;
        this.request = request;
        this.timeoutMillis = timeoutMillis;
        this.invokeCallback = invokeCallback;
        this.once = once;
    }

    public void executeInvokeCallback() {
        if (this.invokeCallback != null && this.executeCallbackOnlyOnce.compareAndSet(false, true)) {
            this.invokeCallback.operationComplete(this);
        }
    }

    public void interrupt() {
        this.interrupted = true;
        this.executeInvokeCallback();
    }

    public void release() {
        if (this.once != null) {
            this.once.release();
        }
    }

    public RemotingCommand waitResponse(final long timeoutMillis) throws InterruptedException {
        this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        return this.response;
    }

    public Channel getChannel() {
        return channel;
    }

    public RemotingCommand getRequest() {
        return request;
    }

    public RemotingCommand getResponse() {
        return response;
    }

    public void setResponse(RemotingCommand response) {
        this.response = response;
    }

    public void putResponse(final RemotingCommand response) {
        this.response = response;
        this.countDownLatch.countDown();
    }

    public boolean isTimeout() {
        return System.currentTimeMillis() - this.beginTimestamp > this.timeoutMillis;
    }

    public boolean isInterrupted() {
        return interrupted;
    }

    public boolean isSendRequestOK() {
        return sendRequestOK;
    }

    public void setSendRequestOK(boolean sendRequestOK) {
        this.sendRequestOK = sendRequestOK;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }

    public InvokeCallback getInvokeCallback() {
        return invokeCallback;
    }
}
