package com.ceilzcx.siestamq.remoting;

/**
 * @author ceilzcx
 * @since 8/12/2022
 */
public interface RemotingService {

    void start();

    void shutdown();

    // 注册钩子函数
    void registerRpcHook(RPCHook rpcHook);

    // 清除全部钩子函数
    void clearRpcHooks();
}
