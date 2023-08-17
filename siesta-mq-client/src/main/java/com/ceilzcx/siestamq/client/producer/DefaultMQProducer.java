package com.ceilzcx.siestamq.client.producer;

import com.ceilzcx.siestamq.client.ClientConfig;
import com.ceilzcx.siestamq.client.exception.MQClientException;
import com.ceilzcx.siestamq.client.impl.producer.DefaultMQProducerImpl;
import com.ceilzcx.siestamq.client.impl.producer.SendCallback;
import com.ceilzcx.siestamq.common.message.Message;
import com.ceilzcx.siestamq.remoting.RPCHook;
import com.ceilzcx.siestamq.remoting.exception.RemotingException;

/**
 * 供用户调用的Producer实现类, 具体Producer的实现类是DefaultMQProducerImpl<br>
 * 不知道为什么ClientConfig作为继承类, 而不是属性, 便于MQClientInstance使用, Producer和Consumer有一个共同的父类<br>
 *
 * @author ceilzcx
 * @since 3/4/2023
 */
public class DefaultMQProducer extends ClientConfig implements MQProducer {

    private final DefaultMQProducerImpl defaultMQProducerImpl;

    // todo 和consumerGroup有什么联系?
    private String producerGroup;

    // 一部分配置项属性
    /**
     * 失败重发消息次数
     */
    private int retryTimesWhenSendFailed = 2;

    /**
     * 默认创建的topic的message queue数量
     */
    private volatile int defaultTopicQueueNums = 4;

    public DefaultMQProducer(String producerGroup) {
        this(null, producerGroup, null);
    }

    public DefaultMQProducer(String namespace, String producerGroup, RPCHook rpcHook) {
        this.namespace = namespace;
        this.producerGroup = producerGroup;
        this.defaultMQProducerImpl = new DefaultMQProducerImpl(this, rpcHook);
    }

    @Override
    public void start() throws MQClientException {
        this.producerGroup = this.withNamespace(producerGroup);
        // start defaultProducerImpl
        this.defaultMQProducerImpl.start();
    }

    @Override
    public void shutdown() {

    }

    @Override
    public void send(Message msg) throws MQClientException, RemotingException {

    }

    @Override
    public void send(Message msg, long timeout) throws MQClientException, RemotingException {

    }

    @Override
    public void send(Message msg, SendCallback sendCallback) throws MQClientException, RemotingException {

    }

    public String withNamespace(String producerGroup) {
        // 源码会调用org.apache.rocketmq.remoting.protocol.NamespaceUtil#wrapNamespace
        // 处理retry(延迟消息)和DLQ(死信消息)
        return producerGroup;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public int getRetryTimesWhenSendFailed() {
        return retryTimesWhenSendFailed;
    }

    public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed) {
        this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
    }

    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }

    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }
}
