package com.ceilzcx.siestamq.remoting.netty;

import com.ceilzcx.siestamq.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author ceilzcx
 * @since 8/12/2022
 *
 * command的处理接口, 根据不同的code处理不同的Processor, 注册到 RemotingServer 或 RemotingClient 中
 * remoting 和 processor实现类 解耦
 */
public interface NettyRequestProcessor {

    // 执行processor, request -> response
    RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception;

    // 是否拒绝request
    boolean rejectRequest();
}
