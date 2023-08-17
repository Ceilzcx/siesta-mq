package com.ceilzcx.siestamq.client.impl.consumer;

import com.ceilzcx.siestamq.client.impl.factory.MQClientInstance;

/**
 * 重负载均衡服务, 类似一个线程服务
 *
 * @author ceilzcx
 * @since 3/4/2023
 */
public class RebalanceService {
    private final MQClientInstance instance;

    public RebalanceService(MQClientInstance instance) {
        this.instance = instance;
    }

    public void start() {
        while (true) {
            // 具体实现类MQClientInstance#doRebalance
        }
    }

    // ServiceThread
}
