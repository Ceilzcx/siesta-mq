package com.ceilzcx.siestamq.client.impl;

import com.ceilzcx.siestamq.remoting.exception.RemotingConnectException;
import com.ceilzcx.siestamq.remoting.exception.RemotingSendRequestException;
import com.ceilzcx.siestamq.remoting.exception.RemotingTimeoutException;
import com.ceilzcx.siestamq.remoting.netty.config.NettyClientConfig;

/**
 * @author ceilzcx
 * @since 16/12/2022
 */
public class MQClientStartup {

    public static void main(String[] args) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        NettyClientConfig nettyClientConfig = new NettyClientConfig();
        MQClientApiImpl mqClientApi = new MQClientApiImpl(nettyClientConfig, null);
        mqClientApi.start();
        mqClientApi.createTopic("0.0.0.0:9876", "test topic", 3000);
    }

}
