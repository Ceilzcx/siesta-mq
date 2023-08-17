package com.ceilzcx.siestamq.broker.out;

import com.ceilzcx.siestamq.broker.task.BrokerFixedThreadPoolExecutor;
import com.ceilzcx.siestamq.common.ThreadFactoryImpl;
import com.ceilzcx.siestamq.remoting.RemotingClient;
import com.ceilzcx.siestamq.remoting.exception.RemotingConnectException;
import com.ceilzcx.siestamq.remoting.exception.RemotingSendRequestException;
import com.ceilzcx.siestamq.remoting.exception.RemotingTimeoutException;
import com.ceilzcx.siestamq.remoting.netty.NettyRemotingClient;
import com.ceilzcx.siestamq.remoting.netty.config.NettyClientConfig;
import com.ceilzcx.siestamq.remoting.protocol.RemotingCommand;
import com.ceilzcx.siestamq.remoting.protocol.RequestCode;
import com.ceilzcx.siestamq.remoting.protocol.body.RegisterBrokerBody;
import com.ceilzcx.siestamq.remoting.protocol.body.TopicConfigSerializeWrapper;
import com.ceilzcx.siestamq.remoting.protocol.header.nameserver.RegisterBrokerRequestHeader;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author ceilzcx
 * @since 17/12/2022
 */
@Slf4j
public class BrokerOuterAPI {
    private final RemotingClient remotingClient;

    private BrokerFixedThreadPoolExecutor brokerOuterExecutor =
            new BrokerFixedThreadPoolExecutor(
                    4,
                    10,
                    1,
                    TimeUnit.MINUTES,
                    new ArrayBlockingQueue<>(32),
                    new ThreadFactoryImpl("brokerOutApi_thread_", true));

    public BrokerOuterAPI(NettyClientConfig nettyClientConfig) {
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);
    }

    public void updateNameserverAddrList(String addr) {
        String[] addrArray = addr.split(",");
        remotingClient.updateNameServerAddrList(new ArrayList<>(Arrays.asList(addrArray)));
    }

    public void registerTopicToNameserver(String topic) {

    }

    public List<String> registerBrokerAll(
            final String clusterName,
            final String brokerAddr,
            final String brokerName,
            final long brokerId,
            final TopicConfigSerializeWrapper topicConfigSerializeWrapper,
            final int timeoutMillis,
            final Long heartbeatTimeoutMillis) {
        // 获取所有nameserver有效地址
        List<String> nameserverList = this.remotingClient.getAvailableNameServerList();
        if (nameserverList != null && !nameserverList.isEmpty()) {
            final RegisterBrokerRequestHeader requestHeader = new RegisterBrokerRequestHeader();
            requestHeader.setClusterName(clusterName);
            requestHeader.setBrokerAddr(brokerAddr);
            requestHeader.setBrokerName(brokerName);
            requestHeader.setBrokerId(brokerId);
            if (heartbeatTimeoutMillis != null) {
                requestHeader.setHeartbeatTimeoutMillis(heartbeatTimeoutMillis);
            }

            RegisterBrokerBody requestBody = new RegisterBrokerBody();
            requestBody.setTopicConfigSerializeWrapper(topicConfigSerializeWrapper);

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REGISTER_BROKER, requestHeader);
            // crc
            request.setBody(requestBody.encode());

            for (String nameserverAddr : nameserverList) {
                this.brokerOuterExecutor.execute(() -> {
                    try {
                        RemotingCommand response = BrokerOuterAPI.this.remotingClient.invokeSync(nameserverAddr, request, timeoutMillis);

                    } catch (InterruptedException | RemotingConnectException |
                            RemotingSendRequestException | RemotingTimeoutException e) {
                        log.error("Failed to register current broker to name server. TargetHost={}", nameserverAddr, e);
                    }
                });
            }
        }

        return null;
    }
}
