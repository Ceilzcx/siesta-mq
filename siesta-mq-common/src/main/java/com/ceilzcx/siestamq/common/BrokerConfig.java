package com.ceilzcx.siestamq.common;

import com.ceilzcx.siestamq.common.utils.NetworkUtil;

/**
 * @author ceilzcx
 * @since 17/12/2022
 */
public class BrokerConfig {
    private static final String DEFAULT_CLUSTER_NAME = "DefaultCluster";

    private static String localHostName;

    private String brokerIP1 = NetworkUtil.getLocalAddress();

    private String brokerName = defaultBrokerName();
    private String brokerClusterName = DEFAULT_CLUSTER_NAME;
    private volatile long brokerId = 0L;
    private boolean isInBrokerContainer = false;

    private String nameserverAddr;

    private int adminBrokerThreadPoolNums = 16;

    private int adminBrokerThreadPoolQueueCapacity = 10000;

    private int registerBrokerTimeoutMills = 24000;

    public String getNameserverAddr() {
        return nameserverAddr;
    }

    public void setNameserverAddr(String nameserverAddr) {
        this.nameserverAddr = nameserverAddr;
    }

    public String getBrokerIP1() {
        return brokerIP1;
    }

    public void setBrokerIP1(String brokerIP1) {
        this.brokerIP1 = brokerIP1;
    }

    public int getAdminBrokerThreadPoolNums() {
        return adminBrokerThreadPoolNums;
    }

    public void setAdminBrokerThreadPoolNums(int adminBrokerThreadPoolNums) {
        this.adminBrokerThreadPoolNums = adminBrokerThreadPoolNums;
    }

    public int getAdminBrokerThreadPoolQueueCapacity() {
        return adminBrokerThreadPoolQueueCapacity;
    }

    public void setAdminBrokerThreadPoolQueueCapacity(int adminBrokerThreadPoolQueueCapacity) {
        this.adminBrokerThreadPoolQueueCapacity = adminBrokerThreadPoolQueueCapacity;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getBrokerClusterName() {
        return brokerClusterName;
    }

    public void setBrokerClusterName(String brokerClusterName) {
        this.brokerClusterName = brokerClusterName;
    }

    public long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }

    public boolean isInBrokerContainer() {
        return isInBrokerContainer;
    }

    public void setInBrokerContainer(boolean inBrokerContainer) {
        isInBrokerContainer = inBrokerContainer;
    }

    public int getRegisterBrokerTimeoutMills() {
        return registerBrokerTimeoutMills;
    }

    public void setRegisterBrokerTimeoutMills(int registerBrokerTimeoutMills) {
        this.registerBrokerTimeoutMills = registerBrokerTimeoutMills;
    }

    private String defaultBrokerName() {
        return (localHostName == null || localHostName.isEmpty()) ? "DEFAULT_BROKER" : localHostName;
    }
}
