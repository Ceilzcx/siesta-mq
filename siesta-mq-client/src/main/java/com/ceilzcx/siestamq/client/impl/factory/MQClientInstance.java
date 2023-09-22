package com.ceilzcx.siestamq.client.impl.factory;

import com.ceilzcx.siestamq.client.ClientConfig;
import com.ceilzcx.siestamq.client.MQAdminImpl;
import com.ceilzcx.siestamq.client.exception.MQClientException;
import com.ceilzcx.siestamq.client.impl.MQClientApiImpl;
import com.ceilzcx.siestamq.client.impl.consumer.PullMessageService;
import com.ceilzcx.siestamq.client.impl.consumer.RebalanceService;
import com.ceilzcx.siestamq.client.impl.producer.DefaultMQProducerImpl;
import com.ceilzcx.siestamq.client.impl.producer.MQProducerInner;
import com.ceilzcx.siestamq.client.producer.DefaultMQProducer;
import com.ceilzcx.siestamq.client.producer.TopicPublishInfo;
import com.ceilzcx.siestamq.common.MixAll;
import com.ceilzcx.siestamq.common.ServiceState;
import com.ceilzcx.siestamq.common.message.MessageQueue;
import com.ceilzcx.siestamq.common.topic.TopicValidator;
import com.ceilzcx.siestamq.remoting.RPCHook;
import com.ceilzcx.siestamq.remoting.netty.config.NettyClientConfig;

import com.ceilzcx.siestamq.remoting.protocol.route.BrokerData;
import com.ceilzcx.siestamq.remoting.protocol.route.QueueData;
import com.ceilzcx.siestamq.remoting.protocol.route.TopicRouteData;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Producer和Consumer应该是共用一个MQClientInstance, 具体参看MQClientManager, 两个clientId应该是一样的
 *
 * @author ceilzcx
 * @since 3/4/2023
 */
public class MQClientInstance {
    private final static Logger log = LoggerFactory.getLogger(MQClientInstance.class);

    private final String clientId;
    private final ClientConfig clientConfig;
    private final NettyClientConfig nettyClientConfig;

    private DefaultMQProducer defaultMQProducer;

    private MQClientApiImpl mqClientApi;
    private MQAdminImpl mqAdmin;
    private PullMessageService pullMessageService;
    private RebalanceService rebalanceService;

    private ServiceState serviceState;

    // key: topic, value: MQProducer
    private final ConcurrentMap<String, MQProducerInner> producerTable = new ConcurrentHashMap<>();

    // key: topic, value: Map[key: message queue, value: broker name]
    // 存储一份快照, 获取topic & message queue 对应的broker name时使用
    // todo 不明白为什么需要加这份快照, MessageQueue中存有brokerName
    private final ConcurrentMap<String, ConcurrentHashMap<MessageQueue, String>> topicEndPointsTable = new ConcurrentHashMap<>();

    // key: topic, value: topic route data
    // 从nameserver获取的route信息, 快照用于减少nameserver处查询, 和判断是否存在更新
    private final ConcurrentMap<String, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();

    // key: broker name, value: Map[key: broker id, value: broker address]
    // 一个brokerName对应多个brokerId? master/slave模式, brokerName一样, master的brokerId=0, slave的brokerId>0
    private final ConcurrentMap<String, HashMap<Long, String>> brokerAddrTable = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "MQClientFactoryScheduledThread"));

    private final Lock lockNameServer = new ReentrantLock();

    public MQClientInstance(ClientConfig clientConfig, String clientId, RPCHook rpcHook) {
        this.clientConfig = clientConfig;

        this.nettyClientConfig = new NettyClientConfig();
        // set client config attrs to netty client config

        this.clientId = clientId;

        // MQClientApiImpl
        this.mqClientApi = new MQClientApiImpl(this.nettyClientConfig, rpcHook);
        // MQAdminImpl
        this.mqAdmin = new MQAdminImpl(this);

        // PullMessageService
        // 只有Push模式的Consumer端会使用到
        this.pullMessageService = new PullMessageService(this);

        // RebalanceService
        // 只有Consumer端会使用到
        this.rebalanceService = new RebalanceService(this);

        // DefaultMQProducer ---> DefaultMQProducerImpl ---> MQClientInstance
        // 为什么还需要创建一个DefaultMQProducer? 内部使用, 主要处理Consumer端消费失败发送消息, 和用户自定义发送消息的区分
        this.defaultMQProducer = new DefaultMQProducer(MixAll.CLIENT_INNER_PRODUCER_GROUP);

        // ConsumerStatsManager
    }

    public void start() throws MQClientException {
        // todo 外层逻辑类似, 为什么他需要锁自己, 而Producer和Consumer不用, 存在多线程?
        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    this.serviceState = ServiceState.START_FAILED;

                    this.mqClientApi.start();

                    this.pullMessageService.start();

                    this.rebalanceService.start();

                    // 启动调度任务
                    this.startScheduledTask();

                    // defaultMQProducerImpl start

                    this.serviceState = ServiceState.RUNNING;
                    break;
                case START_FAILED:
                    throw new MQClientException("The Factory object[" + this.getClientId() + "] has been created before, and failed.", null);
                default:
                    break;
            }
        }
    }

    private void startScheduledTask() {
        // 如果用户没有手动指定, 获取远端的nameserver地址, 实现比较复杂
        if (null == this.clientConfig.getNameserverAddress()) {
            this.scheduledExecutorService.scheduleAtFixedRate(() -> {
                // org.apache.rocketmq.client.impl.MQClientAPIImpl#fetchNameServerAddr
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        }

        // 获取nameserver的TopicRouteData

        // 向所有broker发送心跳

        // 持久化所有的consumer消费的offset

        // todo adjustThreadPool?
    }

    public boolean registerProducer(final String groupName, DefaultMQProducerImpl producer) {
        if (null == groupName || null == producer) {
            return false;
        }
        MQProducerInner prev = this.producerTable.put(groupName, producer);
        if (prev != null) {
            // log.warn
            return false;
        }
        return true;
    }

    public boolean updateTopicRouteInfoFromNameServer(final String topic) {
        return this.updateTopicRouteInfoFromNameServer(topic, false, null);
    }

    public boolean updateTopicRouteInfoFromNameServer(final String topic, boolean isDefault, DefaultMQProducer defaultMQProducer) {
        try {
            if (this.lockNameServer.tryLock()) {
                TopicRouteData topicRouteData;
                if (isDefault && defaultMQProducer != null) {
                    topicRouteData = this.mqClientApi.getDefaultTopicRouteDataFromNameServer(this.clientConfig.getMqClientApiTimeout());
                    if (topicRouteData != null) {
                        for (QueueData queueData : topicRouteData.getQueueDatas()) {
                            int queueNums = Math.min(queueData.getReadQueueNums(), defaultMQProducer.getDefaultTopicQueueNums());
                            queueData.setReadQueueNums(queueNums);
                            queueData.setWriteQueueNums(queueNums);
                        }
                    }
                } else {
                    topicRouteData = this.mqClientApi.getTopicRouteDataFromNameServer(topic, this.clientConfig.getMqClientApiTimeout(), true);
                }

                // 更新需要快照信息
                if (topicRouteData != null) {
                    TopicRouteData oldTopicRouteData = this.topicRouteTable.get(topic);
                    boolean isChange = topicRouteData.topicRouteDataChanged(oldTopicRouteData);
                    if (!isChange) {
                        // todo 这里还有一步校验规则, 校验Producer的TopicPublishInfo是否有效, 不清楚这步操作的含义
                    } else {
                        log.info("the topic[{}] route info changed, old[{}] ,new[{}]", topic, oldTopicRouteData, topicRouteData);
                    }

                    if (isChange) {

                        // 更新brokerAddrTable
                        for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
                            this.brokerAddrTable.put(brokerData.getBrokerName(), brokerData.getBrokerAddrs());
                        }

                        // todo update endpointTable: invoke topicRouteDataToEndpointsForTopic

                        // 更新各个producer的topicPublishInfo


                    }
                }
            }
        } catch (MQClientException e) {
            if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) && !topic.equals(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC)) {
                log.warn("updateTopicRouteInfoFromNameServer Exception", e);
            }
        } catch (Exception e) {
            log.error("updateTopicRouteInfoFromNameServer Exception", e);
            throw new IllegalStateException(e);
        } finally {
            this.lockNameServer.unlock();
        }

        return true;
    }

//    todo 首先不理解为什么要放在remoting模块, 其次对于static topic理解不深刻
//    public static ConcurrentMap<MessageQueue, String> topicRouteDataToEndpointsForTopic(final String topic, final TopicRouteData topicRouteData) {
//    }

    public static TopicPublishInfo topicRouteDataToTopicPublishInfo(final String topic, final TopicRouteData topicRouteData) {
        TopicPublishInfo topicPublishInfo = new TopicPublishInfo();
        topicPublishInfo.setTopicRouteData(topicRouteData);

        // 是否设置了顺序发送消息
        if (topicRouteData.getOrderTopicConf() != null && topicRouteData.getOrderTopicConf().length() > 0) {
            String[] brokers = topicRouteData.getOrderTopicConf().split(";");
            for (String broker : brokers) {
                String[] items = broker.split(":");
                int nums = Integer.parseInt(items[1]);
                for (int i = 0; i < nums; i++) {
                    MessageQueue messageQueue = new MessageQueue(topic, items[0], i);
                    topicPublishInfo.getMessageQueueList().add(messageQueue);
                }
            }
            topicPublishInfo.setOrderTopic(true);
        } else if (topicRouteData.getOrderTopicConf() == null
                && topicRouteData.getTopicQueueMappingByBroker() != null
                && !topicRouteData.getTopicQueueMappingByBroker().isEmpty()) {
            // todo
        } else {

        }


        return topicPublishInfo;
    }

    public String findMasterBrokerAddress(final String brokerName) {
        if (brokerName == null) {
            return null;
        }
        HashMap<Long, String> map = this.brokerAddrTable.get(brokerName);
        if (map != null) {
            return map.get(MixAll.MASTER_ID);
        }
        return null;
    }

    public String getClientId() {
        return clientId;
    }
}
