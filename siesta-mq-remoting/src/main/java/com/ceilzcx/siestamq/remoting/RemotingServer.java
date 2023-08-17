package com.ceilzcx.siestamq.remoting;

import com.ceilzcx.siestamq.common.Pair;
import com.ceilzcx.siestamq.remoting.exception.RemotingSendRequestException;
import com.ceilzcx.siestamq.remoting.exception.RemotingTimeoutException;
import com.ceilzcx.siestamq.remoting.exception.RemotingTooMuchRequestException;
import com.ceilzcx.siestamq.remoting.netty.NettyRequestProcessor;
import com.ceilzcx.siestamq.remoting.protocol.RemotingCommand;
import io.netty.channel.Channel;

import java.util.concurrent.ExecutorService;

/**
 * @author ceilzcx
 * @since 8/12/2022
 */
public interface RemotingServer extends RemotingService {

    void registerProcessor(final int requestCode, final NettyRequestProcessor processor, final ExecutorService executorService);

    // 注册默认的processor, requestCode没找到时使用
    void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executorService);

    int localListenPort();

    Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(int requestCode);

    Pair<NettyRequestProcessor, ExecutorService> getDefaultProcessorPair();

    RemotingServer newRemotingServer(int port);

    void removeRemotingServer(int port);

    RemotingCommand invokeSync(final Channel channel, final RemotingCommand request, final long timeoutMillis)
            throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException;

    // 异步调用
    void invokeAsync(final Channel channel, final RemotingCommand request, final long timeoutMillis, final InvokeCallback invokeCallback)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingSendRequestException, RemotingTimeoutException;

    // 只负责发送消息, 不等待服务器响应且不触发回调函数, 不安全但速度快
    void invokeOneway(final Channel channel, final RemotingCommand request, final long timeoutMillis)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingSendRequestException, RemotingTimeoutException;
}
