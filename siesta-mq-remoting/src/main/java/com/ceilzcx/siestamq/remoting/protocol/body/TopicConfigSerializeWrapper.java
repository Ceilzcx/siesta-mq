package com.ceilzcx.siestamq.remoting.protocol.body;

import com.ceilzcx.siestamq.common.TopicConfig;
import com.ceilzcx.siestamq.remoting.protocol.DataVersion;
import com.ceilzcx.siestamq.remoting.protocol.RemotingSerializable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author ceilzcx
 * @since 19/12/2022
 */
public class TopicConfigSerializeWrapper extends RemotingSerializable {
    private ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();
    private DataVersion dataVersion = new DataVersion();

    public ConcurrentMap<String, TopicConfig> getTopicConfigTable() {
        return topicConfigTable;
    }

    public void setTopicConfigTable(ConcurrentMap<String, TopicConfig> topicConfigTable) {
        this.topicConfigTable = topicConfigTable;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }
}
