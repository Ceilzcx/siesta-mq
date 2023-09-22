package com.ceilzcx.siestamq.client.impl;

import com.ceilzcx.siestamq.client.exception.MQClientException;
import com.ceilzcx.siestamq.client.impl.producer.DefaultMQProducerImpl;
import com.ceilzcx.siestamq.client.impl.producer.SendCallback;
import com.ceilzcx.siestamq.client.producer.SendResult;
import com.ceilzcx.siestamq.common.MixAll;
import com.ceilzcx.siestamq.common.message.Message;
import com.ceilzcx.siestamq.common.message.MessageBatch;
import com.ceilzcx.siestamq.common.message.MessageConstants;
import com.ceilzcx.siestamq.common.topic.TopicValidator;
import com.ceilzcx.siestamq.remoting.InvokeCallback;
import com.ceilzcx.siestamq.remoting.RPCHook;
import com.ceilzcx.siestamq.remoting.RemotingClient;
import com.ceilzcx.siestamq.remoting.exception.RemotingConnectException;
import com.ceilzcx.siestamq.remoting.exception.RemotingSendRequestException;
import com.ceilzcx.siestamq.remoting.exception.RemotingTimeoutException;
import com.ceilzcx.siestamq.remoting.netty.NettyRemotingClient;
import com.ceilzcx.siestamq.remoting.netty.ResponseFuture;
import com.ceilzcx.siestamq.remoting.netty.config.NettyClientConfig;
import com.ceilzcx.siestamq.remoting.protocol.RemotingCommand;
import com.ceilzcx.siestamq.remoting.protocol.RemotingSysResponseCode;
import com.ceilzcx.siestamq.remoting.protocol.RequestCode;
import com.ceilzcx.siestamq.remoting.protocol.ResponseCode;
import com.ceilzcx.siestamq.remoting.protocol.header.broker.CreateTopicRequestHeader;
import com.ceilzcx.siestamq.remoting.protocol.header.broker.SendMessageRequestHeader;
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

        // invoke to get route data
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTE_DATA_BY_TOPIC, requestHeader);

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

    public SendResult sendMessage(
            final String addr,
            final String brokerName,
            final Message message,
            final SendMessageRequestHeader requestHeader,
            final long timeoutMillis,
            final CommunicationMode communicationMode
    ) {
        final long beginTime = System.currentTimeMillis();
        RemotingCommand request;
        String msgType = message.getProperty(MessageConstants.PROPERTY_MESSAGE_TYPE);
        boolean isRetry = MixAll.REPLY_MESSAGE_FLAG.equals(msgType);

        if (isRetry) {
            // todo smart message, V2版本的消息发送


        } else {
            if (message instanceof MessageBatch) {
                // todo headerV2
                request = RemotingCommand.createRequestCommand(RequestCode.SEND_BATCH_MESSAGE, requestHeader);
            } else {
                request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
            }

        }

        return null;
    }

    private void sendMessageAsync(
            final String addr,
            final String brokerName,
            final Message message,
            final long timeoutMillis,
            final RemotingCommand request,
            final SendCallback sendCallback,
            // todo producer 当做入参来使用?
            final DefaultMQProducerImpl producer
    ) {
        final long beginStartTime = System.currentTimeMillis();
        try {
            this.remotingClient.invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {
                @Override
                public void operationComplete(ResponseFuture responseFuture) {

                    RemotingCommand response = responseFuture.getResponse();
                    if (null == sendCallback && null != response) {

                        // 使用默认的sendCallback, 处理response
                        SendResult sendResult = MQClientApiImpl.this.processSendResponse(brokerName, message, response);
                        // todo 由context处理

                        // todo 按照源码成功的信息也会被记录?
                        producer.updateFaultItem(brokerName, System.currentTimeMillis(), false);
                        return;
                    }
                    if (response != null) {

                    }

                }
            });
        } catch (Exception e) {

        }
    }

    // 处理默认的send response
    private SendResult processSendResponse(
            final String brokerName,
            final Message message,
            final RemotingCommand response
            ) {
        // todo 处理response
        return new SendResult();
    }

}
