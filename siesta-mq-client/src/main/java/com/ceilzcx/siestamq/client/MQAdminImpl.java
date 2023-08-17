package com.ceilzcx.siestamq.client;

import com.ceilzcx.siestamq.client.impl.factory.MQClientInstance;

import java.util.Map;

/**
 * 实现类, 主要封装request和回调处理response, 和NameServer或Broker的网络交互还是通过MQClientAPIImpl实现
 *
 * @author ceilzcx
 * @since 3/4/2023
 */
public class MQAdminImpl {
    private final MQClientInstance instance;

    public MQAdminImpl(MQClientInstance instance) {
        this.instance = instance;
    }

    /**
     * 创建topic
     * @param key todo 好像没用到?
     * @param newTopic 需要新建的topic name
     * @param queueNum Message Queue数量
     */
    public void createTopic(String key, String newTopic, int queueNum) {
        this.createTopic(key, newTopic, queueNum, 0, null);
    }

    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag, Map<String, String> attributes) {

    }


    public Object queryMessage(String topic, String key, int maxNum, long begin, long end) {
        return null;
    }


}
