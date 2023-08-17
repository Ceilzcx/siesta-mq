package com.ceilzcx.siestamq.client.impl;

import com.ceilzcx.siestamq.client.ClientConfig;
import com.ceilzcx.siestamq.client.impl.factory.MQClientInstance;
import com.ceilzcx.siestamq.remoting.RPCHook;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 管理MQClientInstance
 *
 * @author ceilzcx
 * @since 3/4/2023
 */
public class MQClientManager {

    private static final MQClientManager instance = new MQClientManager();

    // key: clientId
    private final ConcurrentMap<String, MQClientInstance> instanceTable = new ConcurrentHashMap<>();

    private MQClientManager() {
    }

    public static MQClientManager getInstance() {
        return instance;
    }

    public MQClientInstance getOrCreateMQClientInstance(final ClientConfig clientConfig, RPCHook rpcHook) {
        String clientId = clientConfig.buildClientInstance();
        MQClientInstance instance = instanceTable.get(clientId);
        if (instance == null) {
            instance = new MQClientInstance(clientConfig, clientId, rpcHook);
            this.instanceTable.put(clientId, instance);
        }
        return instance;
    }

}
