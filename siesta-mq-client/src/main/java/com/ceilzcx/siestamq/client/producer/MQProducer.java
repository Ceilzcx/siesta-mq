package com.ceilzcx.siestamq.client.producer;

import com.ceilzcx.siestamq.client.exception.MQClientException;
import com.ceilzcx.siestamq.client.impl.producer.SendCallback;
import com.ceilzcx.siestamq.common.message.Message;
import com.ceilzcx.siestamq.remoting.exception.RemotingException;

/**
 * MQ Producer端接口
 *
 * @author ceilzcx
 * @since 3/4/2023
 */
public interface MQProducer {

    /**
     * 启动Producer
     * @throws MQClientException client端异常
     */
    void start() throws MQClientException;

    void shutdown();

    void send(final Message msg) throws MQClientException, RemotingException;

    /**
     * 同步发送消息
     */
    void send(final Message msg, final long timeout) throws MQClientException, RemotingException;

    /**
     * 异步发送消息
     */
    void send(final Message msg, final SendCallback sendCallback) throws MQClientException, RemotingException;
}
