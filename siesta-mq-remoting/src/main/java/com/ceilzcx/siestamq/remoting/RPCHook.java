package com.ceilzcx.siestamq.remoting;

import com.ceilzcx.siestamq.remoting.protocol.RemotingCommand;

/**
 * @author ceilzcx
 * @since 8/12/2022
 *
 * 钩子函数: 提供request和response的前后处理逻辑接口
 */
public interface RPCHook {

    void doBeforeRequest(final String remoteAddr, final RemotingCommand request);

    void doAfterResponse(final String remoteAddr, final RemotingCommand request, final RemotingCommand response);
}
