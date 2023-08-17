package com.ceilzcx.siestamq.client.impl;

import com.ceilzcx.siestamq.client.exception.MQClientException;
import com.ceilzcx.siestamq.common.topic.TopicValidator;
import com.ceilzcx.siestamq.remoting.RPCHook;
import com.ceilzcx.siestamq.remoting.RemotingClient;
import com.ceilzcx.siestamq.remoting.exception.RemotingConnectException;
import com.ceilzcx.siestamq.remoting.exception.RemotingSendRequestException;
import com.ceilzcx.siestamq.remoting.exception.RemotingTimeoutException;
import com.ceilzcx.siestamq.remoting.netty.NettyRemotingClient;
import com.ceilzcx.siestamq.remoting.netty.config.NettyClientConfig;
import com.ceilzcx.siestamq.remoting.protocol.RemotingCommand;
import com.ceilzcx.siestamq.remoting.protocol.RemotingSysResponseCode;
import com.ceilzcx.siestamq.remoting.protocol.RequestCode;
import com.ceilzcx.siestamq.remoting.protocol.ResponseCode;
import com.ceilzcx.siestamq.remoting.protocol.header.broker.CreateTopicRequestHeader;
import com.ceilzcx.siestamq.remoting.protocol.header.nameserver.GetRouteDataRequestHeader;
import com.ceilzcx.siestamq.remoting.protocol.route.TopicRouteData;
import lombok.extern.slf4j.Slf4j;

/**
 * 真正对外交互的API实现类, 负责和Broker交互
 *
 * @author ceilzcx
 * @since 16/12/2022
 */
@Slf4j
public class MQClientApiImpl {
    private final RemotingClient remotingClient;

    public MQClientApiImpl(NettyClientConfig nettyClientConfig, RPCHook rpcHook) {
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);

        // register rpcHook
        this.remotingClient.registerRpcHook(rpcHook);

        // register processor

    }

    public void start() {
        this.remotingClient.start();
    }

    public void createTopic(final String addr, final String topicName, final long timeoutMillis)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        CreateTopicRequestHeader requestHeader = new CreateTopicRequestHeader();
        requestHeader.setTopic(topicName);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        if (response != null && response.getCode() == RemotingSysResponseCode.SUCCESS) {
            log.info("create topic success");
        } else {
            log.warn("create topic error");
        }
    }

    /**
     * 获取[topic: TBW102]的topicRouteData
     */
    public TopicRouteData getDefaultTopicRouteDataFromNameServer(final long timeoutMillis)
        throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
       return this.getTopicRouteDataFromNameServer(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC, timeoutMillis, false);
    }

    public TopicRouteData getTopicRouteDataFromNameServer(final String topic, final long timeoutMillis,
        boolean allowTopicNotExist) throws MQClientException, RemotingConnectException, RemotingSendRequestException,
        RemotingTimeoutException, InterruptedException {
        GetRouteDataRequestHeader requestHeader = new GetRouteDataRequestHeader();
        requestHeader.setTopic(topic);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.TOPIC_NOT_EXIST:
                if (allowTopicNotExist) {
                    log.warn("get Topic [{}] RouteInfoFromNameServer is not exist value", topic);
                }
                break;
            case ResponseCode.SUCCESS:
                byte[] body = response.getBody();
                if (body != null) {
                    return TopicRouteData.decode(body, TopicRouteData.class);
                }
            default:
                break;
        }
        throw new MQClientException(response.getCode(), response.getRemark());
    }

}
