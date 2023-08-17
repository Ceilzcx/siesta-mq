package com.ceilzcx.siestamq.client.impl.consumer;


import com.ceilzcx.siestamq.client.impl.factory.MQClientInstance;

/**
 * push模式的consumer存储消息类
 * 通过Queue存储消息, 线程池消费消息
 *
 * @author ceilzcx
 * @since 3/4/2023
 */
public class PullMessageService {
    private final MQClientInstance instance;

    public PullMessageService(MQClientInstance instance) {
        this.instance = instance;
    }

    public void start() {

    }

}
