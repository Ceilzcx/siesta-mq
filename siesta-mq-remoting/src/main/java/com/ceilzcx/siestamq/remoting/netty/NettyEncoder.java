package com.ceilzcx.siestamq.remoting.netty;

import com.ceilzcx.siestamq.remoting.common.RemotingHelper;
import com.ceilzcx.siestamq.remoting.protocol.RemotingCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * @author ceilzcx
 * @since 13/12/2022
 *
 * 处理 msg -> byte[]
 * 加上对应的协议
 */
public class NettyEncoder extends MessageToByteEncoder<RemotingCommand> {
    @Override
    protected void encode(ChannelHandlerContext ctx, RemotingCommand remotingCommand, ByteBuf out) throws Exception {
        try {
            remotingCommand.fastEncodeHeader(out);
            byte[] body = remotingCommand.getBody();
            if (body != null) {
                out.writeBytes(body);
            }
        } catch (Exception e) {
            if (remotingCommand != null) {
//                log.error(remotingCommand.toString());
            }
            RemotingHelper.closeChannel(ctx.channel());
        }
    }
}
