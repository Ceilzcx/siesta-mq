package com.ceilzcx.siestamq.remoting.common;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * @author ceilzcx
 * @since 9/12/2022
 */
public class RemotingHelper {
    private static final AttributeKey<String> REMOTE_ADDR_KEY = AttributeKey.valueOf("RemoteAddr");

    private RemotingHelper() {
    }

    public static String parseSocketAddressAddr(SocketAddress socketAddress) {
        if (socketAddress != null) {
            // Default toString of InetSocketAddress is "hostName/IP:port"
            final String addr = socketAddress.toString();
            int index = addr.lastIndexOf("/");
            return (index != -1) ? addr.substring(index + 1) : addr;
        }
        return "";
    }

    public static String parseChannelRemoteAddr(final Channel channel) {
        if (null == channel) {
            return "";
        }
        Attribute<String> att = channel.attr(REMOTE_ADDR_KEY);
        if (att == null) {
            // mocked in unit test
            return parseChannelRemoteAddr0(channel);
        }
        String addr = att.get();
        if (addr == null) {
            addr = parseChannelRemoteAddr0(channel);
            att.set(addr);
        }
        return addr;
    }

    private static String parseChannelRemoteAddr0(final Channel channel) {
        SocketAddress remote = channel.remoteAddress();
        final String addr = remote != null ? remote.toString() : "";

        if (addr.length() > 0) {
            int index = addr.lastIndexOf("/");
            if (index >= 0) {
                return addr.substring(index + 1);
            }

            return addr;
        }

        return "";
    }

    public static int parseSocketAddressPort(SocketAddress socketAddress) {
        if (socketAddress instanceof InetSocketAddress) {
            return ((InetSocketAddress) socketAddress).getPort();
        }
        return -1;
    }

    public static void closeChannel(Channel channel) {
        final String addrRemote = RemotingHelper.parseChannelRemoteAddr(channel);
        if ("".equals(addrRemote)) {
            channel.close();
        } else {
            channel.close().addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
//                    log.info("closeChannel: close the connection to remote address[{}] result: {}", addrRemote,
//                            future.isSuccess());
                }
            });
        }
    }
}