package com.ceilzcx.siestamq.broker;

import com.ceilzcx.siestamq.broker.out.BrokerOuterAPI;
import com.ceilzcx.siestamq.broker.processor.AdminBrokerProcessor;
import com.ceilzcx.siestamq.broker.task.BrokerFixedThreadPoolExecutor;
import com.ceilzcx.siestamq.broker.topic.TopicConfigManager;
import com.ceilzcx.siestamq.common.BrokerConfig;
import com.ceilzcx.siestamq.common.BrokerIdentity;
import com.ceilzcx.siestamq.common.ThreadFactoryImpl;
import com.ceilzcx.siestamq.common.TopicConfig;
import com.ceilzcx.siestamq.remoting.RemotingServer;
import com.ceilzcx.siestamq.remoting.netty.NettyRemotingServer;
import com.ceilzcx.siestamq.remoting.netty.config.NettyClientConfig;
import com.ceilzcx.siestamq.remoting.netty.config.NettyServerConfig;
import com.ceilzcx.siestamq.remoting.protocol.body.TopicConfigSerializeWrapper;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author ceilzcx
 * @since 16/12/2022
 */
@Slf4j
public class BrokerController {
    private final BrokerConfig brokerConfig;
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;

    private RemotingServer remotingServer;

    private final TopicConfigManager topicConfigManager;

    private BrokerOuterAPI brokerOuterAPI;

    protected final BlockingQueue<Runnable> adminBrokerThreadPoolQueue;

    protected ExecutorService adminBrokerExecutor;
    protected ScheduledExecutorService scheduledExecutorService;

    public BrokerController(
            final BrokerConfig brokerConfig,
            final NettyServerConfig nettyServerConfig,
            final NettyClientConfig nettyClientConfig
    ) {
        this.brokerConfig = brokerConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;

        if (nettyClientConfig != null) {
            this.brokerOuterAPI = new BrokerOuterAPI(nettyClientConfig);
        }

        this.adminBrokerThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getAdminBrokerThreadPoolQueueCapacity());

        this.topicConfigManager = new TopicConfigManager();
    }

    public boolean init() {

        this.initializeRemotingServer();

        this.registerProcessor();

        this.initializeResources();

        this.initializeScheduledTasks();

        return true;
    }

    protected void initializeRemotingServer() {
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig);

        // todo fastRemotingServer?
    }

    // 创建各种executor
    protected void initializeResources() {
        this.adminBrokerExecutor = new BrokerFixedThreadPoolExecutor(
                this.brokerConfig.getAdminBrokerThreadPoolNums(),
                this.brokerConfig.getAdminBrokerThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.adminBrokerThreadPoolQueue,
                new ThreadFactoryImpl("AdminBrokerThread_", getBrokerIdentity()));

        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
    }

    protected void initializeScheduledTasks() {
        if (this.brokerConfig.getNameserverAddr() != null) {
            this.updateNameserverAddr();
            this.scheduledExecutorService.scheduleAtFixedRate(() -> {
                try {
                    BrokerController.this.updateNameserverAddr();
                } catch (Throwable e) {
                    log.error("Failed to update nameServer address list", e);
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        }
    }

    protected void registerProcessor() {
        this.remotingServer.registerDefaultProcessor(new AdminBrokerProcessor(this), this.adminBrokerExecutor);
    }

    public BrokerIdentity getBrokerIdentity() {
        return new BrokerIdentity(
                brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(),
                brokerConfig.getBrokerId(), brokerConfig.isInBrokerContainer());
    }

    public synchronized void registerIncrementBrokerData(TopicConfig topicConfig) {
        this.registerIncrementBrokerData(Collections.singletonList(topicConfig));
    }

    public synchronized void registerIncrementBrokerData(List<TopicConfig> topicConfigList) {
        if (topicConfigList == null || topicConfigList.isEmpty()) {
            return;
        }

        TopicConfigSerializeWrapper topicConfigWrapper = new TopicConfigSerializeWrapper();

        ConcurrentMap<String, TopicConfig> topicConfigTable = topicConfigList.stream()
                .collect(Collectors.toConcurrentMap(TopicConfig::getTopicName, Function.identity()));

        topicConfigWrapper.setTopicConfigTable(topicConfigTable);

        this.doRegisterBrokerAll(topicConfigWrapper);
    }

    protected void doRegisterBrokerAll(TopicConfigSerializeWrapper topicConfigWrapper) {
        // 判断是否已经shutdown

        this.brokerOuterAPI.registerBrokerAll(
                this.brokerConfig.getBrokerClusterName(),
                this.getBrokerAddr(),
                this.brokerConfig.getBrokerName(),
                this.brokerConfig.getBrokerId(),
                topicConfigWrapper,
                this.brokerConfig.getRegisterBrokerTimeoutMills(),
                null
        );
    }

    // 修改nameserver的目的, broker将信息注册到nameserver的时候, 需要遍历addr list通信使用
    private void updateNameserverAddr() {
        this.brokerOuterAPI.updateNameserverAddrList(this.brokerConfig.getNameserverAddr());
    }

    public String getBrokerAddr() {
        return this.brokerConfig.getBrokerIP1() + ":" + this.nettyServerConfig.getListenPort();
    }

    public TopicConfigManager getTopicConfigManager() {
        return topicConfigManager;
    }
}
