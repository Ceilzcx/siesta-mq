package com.ceilzcx.siestamq.client.impl.producer;

import com.ceilzcx.siestamq.client.exception.MQClientException;
import com.ceilzcx.siestamq.client.impl.CommunicationMode;
import com.ceilzcx.siestamq.client.impl.MQClientManager;
import com.ceilzcx.siestamq.client.impl.factory.MQClientInstance;
import com.ceilzcx.siestamq.client.latency.MQFaultStrategy;
import com.ceilzcx.siestamq.client.producer.DefaultMQProducer;
import com.ceilzcx.siestamq.client.producer.SendResult;
import com.ceilzcx.siestamq.client.producer.TopicPublishInfo;
import com.ceilzcx.siestamq.common.ServiceState;
import com.ceilzcx.siestamq.common.message.Message;
import com.ceilzcx.siestamq.common.message.MessageQueue;
import com.ceilzcx.siestamq.remoting.RPCHook;
import com.ceilzcx.siestamq.remoting.exception.RemotingException;

/**
 * @author ceilzcx
 * @since 3/4/2023
 */
public class DefaultMQProducerImpl implements MQProducerInner {

    private ServiceState serviceState = ServiceState.CREATE_JUST;

    private final DefaultMQProducer defaultMQProducer;
    private final RPCHook rpcHook;

    private MQClientInstance mQClientFactory;

    private MQFaultStrategy mqFaultStrategy = new MQFaultStrategy();

    public DefaultMQProducerImpl(DefaultMQProducer defaultMQProducer, RPCHook rpcHook) {
        this.defaultMQProducer = defaultMQProducer;
        this.rpcHook = rpcHook;
    }

    public void start() throws MQClientException {
        this.start(true);
    }

    public void start(final boolean startFactory) throws MQClientException {
        switch (this.serviceState) {
            case CREATE_JUST:
                // 先设置为失败, 等全部执行完后会设置为RUNNING
                this.serviceState = ServiceState.START_FAILED;

                this.checkConfig();

                // todo change instance name?

                // get or create instance
                this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQProducer, this.rpcHook);

                // producer name 只能注册一次
                boolean registerOK = this.mQClientFactory.registerProducer(this.defaultMQProducer.getProducerGroup(), this);
                if (!registerOK) {
                    throw new MQClientException("The producer group[" + this.defaultMQProducer.getProducerGroup()
                            + "] has been created before, specify another name please.", null);
                }

                // todo topicPublishInfoTable的用途?

                // need start mq client instance
                if (startFactory) {
                    this.mQClientFactory.start();
                }

                this.serviceState = ServiceState.RUNNING;
                break;
        }

        // 存储并执行过期的request
    }

    private void checkConfig() {
        // 只检查了producerGroup
        // 长度限制, 非法字符, 不等于"DEFAULT_PRODUCER"
    }

    private SendResult sendDefaultImpl(
            Message message,
            final CommunicationMode communicationMode,
            final SendCallback callback,
            final long timeout)
            throws MQClientException, RemotingException {
        // 1.check serviceState == RUNNING
        this.makeSureStateOK();
        // 2. check message, topic != null && body != null && body.length > 0 && body.length < maxSize
        assert message != null && message.getBody() != null;

        long beginTimestampFirst = System.currentTimeMillis();
        long beginTimestampPrev = beginTimestampFirst;

        // 3. find topicPublishInfo: 获取Queue和broker信息
        TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(message.getTopic());

        if (null != topicPublishInfo && topicPublishInfo.ok()) {
            // 记录上一次选择的messageQueue
            MessageQueue messageQueue = null;
            // timesTotal: 同步发送消息 = 1(第一次发送的消息) + retryTimesWhenSendFailed(失败重发的消息), 异步发送消息 = 1
            int timesTotal = CommunicationMode.ASYNC == communicationMode ? 1 + this.defaultMQProducer.getRetryTimesWhenSendFailed() : 1;
            int times = 0;

            for (; times < timesTotal; times++) {
                String lastBrokerName = messageQueue != null ? messageQueue.getBrokerName() : null;

                // 根据策略选择一个MessageQueue
                MessageQueue mqSelected = this.selectOneMessageQueue(topicPublishInfo, lastBrokerName);

                if (null != mqSelected) {
                    messageQueue = mqSelected;

                    // todo 记录每一步的消耗时间, 和timeout比较

                    try {
                        beginTimestampPrev = System.currentTimeMillis();
                        if (times > 0) {
                            // todo 重发的时候重置topic的namespace?
                        }
                        // 判断前面的cost是否大于timeout
                        long costTime = beginTimestampPrev - beginTimestampFirst;
                        if (timeout < costTime) {
                            // todo 处理timeout
                            break;
                        }

                        SendResult sendResult = this.sendKernelImpl(messageQueue, communicationMode, callback, topicPublishInfo, timeout);

                        // todo updateFaultItem
                        this.updateFaultItem(messageQueue.getBrokerName(), System.currentTimeMillis() - beginTimestampFirst, false);

                        switch (communicationMode) {
                            case ONEWAY:
                            case ASYNC:
                                // ONEWAY只发送消息, 不关心返回; ASYNC的异步在callback中处理
                                return null;
                            case SYNC:

                                return sendResult;
                        }

                    } catch (RemotingException | MQClientException e) {
                        // todo 先简单处理exception
                        throw e;
                    }
                }
            }

        }

        return null;
    }

    private SendResult sendKernelImpl(
            final MessageQueue messageQueue,
            final CommunicationMode communicationMode,
            final SendCallback sendCallback,
            final TopicPublishInfo topicPublishInfo,
            final long timeout) throws MQClientException, RemotingException {

        return null;
    }

    private void makeSureStateOK() throws MQClientException {
        if (ServiceState.RUNNING != this.serviceState) {
            throw new MQClientException("The producer service state not OK, "
                    + this.serviceState,
                    null);
        }
    }

    private TopicPublishInfo tryToFindTopicPublishInfo(String topic) {
        return null;
    }

    private MessageQueue selectOneMessageQueue(TopicPublishInfo topicPublishInfo, String lastBrokerName) {
        return this.mqFaultStrategy.selectOneMessageQueue(topicPublishInfo, lastBrokerName);
    }

    private void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        this.mqFaultStrategy.updateFaultItem(brokerName, currentLatency, isolation);
    }

}
