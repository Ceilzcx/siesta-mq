package com.ceilzcx.siestamq.remoting;

import io.netty.channel.Channel;

/**
 * @author ceilzcx
 * @since 8/12/2022
 *
 * 处理部分NettyEvent事件的listener
 */
public interface ChannelEventListener {

    void onChannelConnect(final String remoteAddr, final Channel channel);

    void onChannelClose(final String remoteAddr, final Channel channel);

    void onChannelException(final String remoteAddr, final Channel channel);

    void onChannelIdle(final String remoteAddr, final Channel channel);
}
