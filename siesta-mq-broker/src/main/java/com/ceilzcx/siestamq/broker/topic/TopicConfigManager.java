package com.ceilzcx.siestamq.broker.topic;

import com.ceilzcx.siestamq.common.TopicConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author ceilzcx
 * @since 19/12/2022
 */
public class TopicConfigManager {

    private final Map<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();

    public void updateTopicConfig(TopicConfig topicConfig) {

        // update attributes

        topicConfigTable.put(topicConfig.getTopicName(), topicConfig);

        // persist
    }

}
