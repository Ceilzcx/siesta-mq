package com.ceilzcx.siestamq.remoting.netty;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

/**
 * @author ceilzcx
 * @since 13/12/2022
 *
 * todo 为什么使用ChannelDuplexHandler类?
 *
 * 应该是统计使用, 统计channel的读写数量
 */
@ChannelHandler.Sharable
public class RemotingCodeDistributionHandler extends ChannelDuplexHandler {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // countInbound

        // 从当前ctx开始寻找链式handler的下一个节点
        // 等价 super.channelRead(ctx, msg);
        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        // countOutbound

        super.write(ctx, msg, promise);
    }
}
