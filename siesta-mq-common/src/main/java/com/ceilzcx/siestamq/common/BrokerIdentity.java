package com.ceilzcx.siestamq.common;

import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author ceilzcx
 * @since 19/12/2022
 */
@Slf4j
public class BrokerIdentity {
    private static final String DEFAULT_CLUSTER_NAME = "DefaultCluster";

    private static String localHostName;

    static {
        try {
            localHostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.error("Failed to obtain the host name", e);
        }
    }

    // load it after the localHostName is initialized
    public static final BrokerIdentity BROKER_CONTAINER_IDENTITY = new BrokerIdentity(true);

    private String brokerName = defaultBrokerName();

    private String brokerClusterName = DEFAULT_CLUSTER_NAME;

    private volatile long brokerId = 0L;

    private boolean isBrokerContainer = false;

    // Do not set it manually, it depends on the startup mode
    // Broker start by BrokerStartup is false, start or add by BrokerContainer is true
    private boolean isInBrokerContainer = false;

    public BrokerIdentity() {
    }

    public BrokerIdentity(boolean isBrokerContainer) {
        this.isBrokerContainer = isBrokerContainer;
    }

    public BrokerIdentity(String brokerClusterName, String brokerName, long brokerId) {
        this.brokerName = brokerName;
        this.brokerClusterName = brokerClusterName;
        this.brokerId = brokerId;
    }

    public BrokerIdentity(String brokerClusterName, String brokerName, long brokerId, boolean isInBrokerContainer) {
        this.brokerName = brokerName;
        this.brokerClusterName = brokerClusterName;
        this.brokerId = brokerId;
        this.isInBrokerContainer = isInBrokerContainer;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(final String brokerName) {
        this.brokerName = brokerName;
    }

    public String getBrokerClusterName() {
        return brokerClusterName;
    }

    public void setBrokerClusterName(final String brokerClusterName) {
        this.brokerClusterName = brokerClusterName;
    }

    public long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(final long brokerId) {
        this.brokerId = brokerId;
    }

    public boolean isInBrokerContainer() {
        return isInBrokerContainer;
    }

    public void setInBrokerContainer(boolean inBrokerContainer) {
        isInBrokerContainer = inBrokerContainer;
    }

    private String defaultBrokerName() {
        return (localHostName == null || localHostName.isEmpty()) ? "DEFAULT_BROKER" : localHostName;
    }

    public String getCanonicalName() {
        return isBrokerContainer ? "BrokerContainer" : String.format("%s_%s_%d", brokerClusterName, brokerName,
                brokerId);
    }

    public String getIdentifier() {
        return "#" + getCanonicalName() + "#";
    }
}
