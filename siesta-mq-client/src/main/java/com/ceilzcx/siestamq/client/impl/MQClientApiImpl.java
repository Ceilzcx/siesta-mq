package com.ceilzcx.siestamq.client.impl;

import com.ceilzcx.siestamq.client.exception.MQBrokerException;
import com.ceilzcx.siestamq.client.exception.MQClientException;
import com.ceilzcx.siestamq.client.hook.SendMessageContext;
import com.ceilzcx.siestamq.client.impl.factory.MQClientInstance;
import com.ceilzcx.siestamq.client.impl.producer.DefaultMQProducerImpl;
import com.ceilzcx.siestamq.client.impl.producer.SendCallback;
import com.ceilzcx.siestamq.client.producer.SendResult;
import com.ceilzcx.siestamq.client.producer.SendStatus;
import com.ceilzcx.siestamq.client.producer.TopicPublishInfo;
import com.ceilzcx.siestamq.common.MixAll;
import com.ceilzcx.siestamq.common.message.Message;
import com.ceilzcx.siestamq.common.message.MessageBatch;
import com.ceilzcx.siestamq.common.message.MessageClientIDSetter;
import com.ceilzcx.siestamq.common.message.MessageConstants;
import com.ceilzcx.siestamq.common.message.MessageQueue;
import com.ceilzcx.siestamq.common.topic.TopicValidator;
import com.ceilzcx.siestamq.remoting.InvokeCallback;
import com.ceilzcx.siestamq.remoting.RPCHook;
import com.ceilzcx.siestamq.remoting.RemotingClient;
import com.ceilzcx.siestamq.remoting.exception.RemotingCommandException;
import com.ceilzcx.siestamq.remoting.exception.RemotingConnectException;
import com.ceilzcx.siestamq.remoting.exception.RemotingSendRequestException;
import com.ceilzcx.siestamq.remoting.exception.RemotingTimeoutException;
import com.ceilzcx.siestamq.remoting.exception.RemotingTooMuchRequestException;
import com.ceilzcx.siestamq.remoting.netty.NettyRemotingClient;
import com.ceilzcx.siestamq.remoting.netty.ResponseFuture;
import com.ceilzcx.siestamq.remoting.netty.config.NettyClientConfig;
import com.ceilzcx.siestamq.remoting.protocol.RemotingCommand;
import com.ceilzcx.siestamq.remoting.protocol.RemotingSysResponseCode;
import com.ceilzcx.siestamq.remoting.protocol.RequestCode;
import com.ceilzcx.siestamq.remoting.protocol.ResponseCode;
import com.ceilzcx.siestamq.remoting.protocol.header.broker.CreateTopicRequestHeader;
import com.ceilzcx.siestamq.remoting.protocol.header.broker.SendMessageRequestHeader;
import com.ceilzcx.siestamq.remoting.protocol.header.broker.SendMessageResponseHeader;
import com.ceilzcx.siestamq.remoting.protocol.header.nameserver.GetRouteDataRequestHeader;
import com.ceilzcx.siestamq.remoting.protocol.route.TopicRouteData;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;

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

    /**
     * 调用netty交互broker, 创建topic
     */
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

    public TopicRouteData getTopicRouteDataFromNameServer(final String topic, final long timeoutMillis, boolean allowTopicNotExist)
            throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
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
            final CommunicationMode communicationMode,
            final SendCallback sendCallback,
            final TopicPublishInfo topicPublishInfo,
            final MQClientInstance mqClientInstance,
            final int retryTimesWhenSendFailed,
            final SendMessageContext context,
            // todo producer 当做入参来使用, 感觉有点不太合理, 直接用context.getProducer()也可以?
            final DefaultMQProducerImpl producer
    ) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
            RemotingTooMuchRequestException, RemotingCommandException, MQBrokerException {
        final long beginTime = System.currentTimeMillis();
        RemotingCommand request;
        String msgType = message.getProperty(MessageConstants.PROPERTY_MESSAGE_TYPE);
        boolean isRetry = MixAll.REPLY_MESSAGE_FLAG.equals(msgType);

        if (isRetry) {
            // todo smart message, V2版本的消息发送

            request = RemotingCommand.createRequestCommand(RequestCode.SEND_BATCH_MESSAGE, requestHeader);
        } else {
            if (message instanceof MessageBatch) {
                // todo headerV2
                request = RemotingCommand.createRequestCommand(RequestCode.SEND_BATCH_MESSAGE, requestHeader);
            } else {
                request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
            }
        }
        request.setBody(message.getBody());

        switch (communicationMode) {
            case ONEWAY:
                this.remotingClient.invokeOneway(addr, request, timeoutMillis);
                return null;
            case SYNC:
                long costTimeSync = System.currentTimeMillis() - beginTime;
                if (timeoutMillis < costTimeSync) {
                    throw new RemotingTooMuchRequestException("sendMessage call timeout");
                }
                return this.sendMessageSync(addr, brokerName, message, timeoutMillis, request);
            case ASYNC:
                long costTimeAsync = System.currentTimeMillis() - beginTime;
                if (timeoutMillis < costTimeAsync) {
                    throw new RemotingTooMuchRequestException("sendMessage call timeout");
                }
                final AtomicInteger times = new AtomicInteger();
                this.sendMessageAsync(addr, brokerName, message, timeoutMillis, request, sendCallback, topicPublishInfo,
                        mqClientInstance, retryTimesWhenSendFailed, times, context, producer);
                return null;
        }

        return null;
    }

    private SendResult sendMessageSync(
            final String addr,
            final String brokerName,
            final Message message,
            final long timeoutMillis,
            final RemotingCommand request
    ) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
            RemotingCommandException, MQBrokerException {
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        return this.processSendResponse(brokerName, message, response, addr);
    }

    private void sendMessageAsync(
            final String addr,
            final String brokerName,
            final Message message,
            final long timeoutMillis,
            final RemotingCommand request,
            final SendCallback sendCallback,
            // 发送失败重新发送选出新的broker的时候使用
            final TopicPublishInfo topicPublishInfo,
            final MQClientInstance mqClientInstance,
            // 消息发送失败最大的重试次数
            final int retryTimesWhenSendFailed,
            // 当前第几次执行(重试次数)
            final AtomicInteger times,
            final SendMessageContext context,
            final DefaultMQProducerImpl producer
    ) {
        final long beginStartTime = System.currentTimeMillis();
        try {
            this.remotingClient.invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {
                @Override
                public void operationComplete(ResponseFuture responseFuture) {
                    long cost = System.currentTimeMillis() - beginStartTime;
                    RemotingCommand response = responseFuture.getResponse();
                    if (null == sendCallback && null != response) {

                        // 使用默认的sendCallback, 处理response
                        try {

                            SendResult sendResult = MQClientApiImpl.this.processSendResponse(brokerName, message, response, addr);

                            if (context != null && sendResult != null) {
                                context.setSendResult(sendResult);
                                context.getProducer().executeSendMessageHookAfter(context);
                            }

                        } catch (Exception ignore) {
                        }

                        // todo 按照源码成功的信息也会被记录?
                        producer.updateFaultItem(brokerName, System.currentTimeMillis(), false);
                        return;
                    }
                    if (response != null) {

                        try {
                            SendResult sendResult = MQClientApiImpl.this.processSendResponse(brokerName, message, response, addr);
                            assert sendResult != null;
                            if (context != null) {
                                context.setSendResult(sendResult);
                                context.getProducer().executeSendMessageHookAfter(context);
                            }

                            // 异步处理结果
                            try {
                                sendCallback.onSuccess(sendResult);
                            } catch (Exception ignore) {
                            }

                            producer.updateFaultItem(brokerName, System.currentTimeMillis() - responseFuture.getBeginTimestamp(), false);
                        } catch (Exception e) {
                            // todo 这里感觉不会进来
                            MQClientApiImpl.this.onExceptionImpl(brokerName, message, timeoutMillis - cost, request,
                                    sendCallback, topicPublishInfo, mqClientInstance, retryTimesWhenSendFailed, times, e,
                                    context, producer, false);
                        }

                    } else {
                        producer.updateFaultItem(brokerName, System.currentTimeMillis() - responseFuture.getBeginTimestamp(), false);

                        MQClientException exception;
                        if (!responseFuture.isSendRequestOK()) {
                            exception = new MQClientException("send request failed", responseFuture.getCause());
                        } else if (responseFuture.isTimeout()) {
                            exception = new MQClientException("wait response timeout " + responseFuture.getTimeoutMillis() + "ms",
                                    responseFuture.getCause());
                        } else {
                            exception = new MQClientException("wait response timeout " + responseFuture.getTimeoutMillis() + "ms",
                                    responseFuture.getCause());
                        }
                        MQClientApiImpl.this.onExceptionImpl(brokerName, message, timeoutMillis - cost, request,
                                sendCallback, topicPublishInfo, mqClientInstance, retryTimesWhenSendFailed, times, exception,
                                context, producer, true);
                    }
                }
            });
        } catch (Exception e) {
            long cost = System.currentTimeMillis() - beginStartTime;
            producer.updateFaultItem(brokerName, cost, true);
            this.onExceptionImpl(brokerName, message, timeoutMillis - cost, request, sendCallback, topicPublishInfo,
                    mqClientInstance, retryTimesWhenSendFailed, times, e, context, producer, true);
        }
    }

    /**
     * 默认处理send message response的逻辑
     * 将response和header的信息转为SendResult
     */
    private SendResult processSendResponse(
            final String brokerName,
            final Message message,
            final RemotingCommand response,
            final String address
    ) throws MQBrokerException, RemotingCommandException {
        SendStatus sendStatus;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS:
                sendStatus = SendStatus.SEND_OK;
                break;
            case ResponseCode.FLUSH_DISK_TIMEOUT:
                sendStatus = SendStatus.FLUSH_DISK_TIMEOUT;
            case ResponseCode.FLUSH_SLAVE_TIMEOUT:
                sendStatus = SendStatus.FLUSH_SLAVE_TIMEOUT;
                break;
            case ResponseCode.SLAVE_NOT_AVAILABLE:
                sendStatus = SendStatus.SLAVE_NOT_AVAILABLE;
                break;
            default: {
                throw new MQBrokerException(response.getCode(), response.getRemark(), address);
            }
        }

        String topic = message.getTopic();

        SendMessageResponseHeader responseHeader =
                (SendMessageResponseHeader) response.decodeCommandCustomHeader(SendMessageResponseHeader.class);

        // todo topic add namespace?

        MessageQueue messageQueue = new MessageQueue(topic, brokerName, responseHeader.getQueueId());

        String uniqID = MessageClientIDSetter.getUniqID(message);
        SendResult sendResult = new SendResult(sendStatus, uniqID, responseHeader.getMsgId(), messageQueue, responseHeader.getQueueOffset());

        sendResult.setTransactionId(responseHeader.getTransactionId());
        // todo regionId 和 traceOn

        return sendResult;
    }

    /**
     * 处理消息发送异常情况, 处理sendCallback.onException, 还需要考虑到消息重试的情况
     */
    private void onExceptionImpl(
            final String brokerName,
            final Message message,
            final long timeoutMillis,
            final RemotingCommand request,
            final SendCallback sendCallback,
            final TopicPublishInfo topicPublishInfo,
            final MQClientInstance mqClientInstance,
            final int retryTimesWhenSendFailed,
            final AtomicInteger times,
            final Exception e,
            final SendMessageContext context,
            final DefaultMQProducerImpl producer,
            final boolean needRetry
    ) {
        int curTimes = times.get();
        if (needRetry && curTimes <= retryTimesWhenSendFailed) {
            String retryBrokerName = brokerName;
            if (topicPublishInfo != null) {
                // 选取新的broker
                MessageQueue messageQueue = producer.selectOneMessageQueue(topicPublishInfo, retryBrokerName);
                retryBrokerName = mqClientInstance.getBrokerNameFromMessageQueue(messageQueue);
            }
            String addr = mqClientInstance.findMasterBrokerAddress(brokerName);
            log.warn("async send msg by retry {} times. topic={}, brokerAddr={}, brokerName={}",
                    curTimes, message.getTopic(), addr, retryBrokerName, e);
            // 重发需要添加一些新的requestId
            request.setOpaque(RemotingCommand.createNewRequestId());
            this.sendMessageAsync(addr, retryBrokerName, message, timeoutMillis, request, sendCallback,
                    topicPublishInfo, mqClientInstance, retryTimesWhenSendFailed, times, context, producer);
        } else {

            if (context != null) {
                context.setException(e);
                context.getProducer().executeSendMessageHookAfter(context);
            }

            try {
                sendCallback.onException(e);
            } catch (Exception ignore) {
            }
        }
    }

}
