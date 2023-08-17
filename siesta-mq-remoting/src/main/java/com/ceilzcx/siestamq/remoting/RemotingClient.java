package com.ceilzcx.siestamq.remoting;

import com.ceilzcx.siestamq.remoting.exception.RemotingConnectException;
import com.ceilzcx.siestamq.remoting.exception.RemotingSendRequestException;
import com.ceilzcx.siestamq.remoting.exception.RemotingTimeoutException;
import com.ceilzcx.siestamq.remoting.exception.RemotingTooMuchRequestException;
import com.ceilzcx.siestamq.remoting.netty.NettyRequestProcessor;
import com.ceilzcx.siestamq.remoting.protocol.RemotingCommand;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * @author ceilzcx
 * @since 13/12/2022
 */
public interface RemotingClient extends RemotingService {

    void updateNameServerAddrList(final List<String> addrList);

    List<String> getNameServerAddressList();

    List<String> getAvailableNameServerList();

    RemotingCommand invokeSync(final String addr, final RemotingCommand request,
                               final long timeoutMillis) throws InterruptedException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException;

    /**
     * 相比于server的区别, 入参channel ---> addr, 由client内部通过addr获取对应的channel
     */
    void invokeAsync(final String addr, final RemotingCommand request, final long timeoutMillis,
                     final InvokeCallback invokeCallback) throws InterruptedException, RemotingConnectException,
            RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    void invokeOneway(final String addr, final RemotingCommand request, final long timeoutMillis)
            throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException,
            RemotingTimeoutException, RemotingSendRequestException;

    void registerProcessor(final int requestCode, final NettyRequestProcessor processor, final ExecutorService executor);

    void setCallbackExecutor(final ExecutorService executorService);

    boolean isChannelWritable(final String addr);

    /**
     * 关闭指定地址的channel, 关闭连接
     */
    void closeChannels(final List<String> addrList);
}
