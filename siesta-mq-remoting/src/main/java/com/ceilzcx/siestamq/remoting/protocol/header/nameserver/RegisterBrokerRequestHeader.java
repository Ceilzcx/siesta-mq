package com.ceilzcx.siestamq.remoting.protocol.header.nameserver;

import com.ceilzcx.siestamq.remoting.CommandCustomHeader;
import com.ceilzcx.siestamq.remoting.exception.RemotingCommandException;

/**
 * @author ceilzcx
 * @since 20/12/2022
 */
public class RegisterBrokerRequestHeader implements CommandCustomHeader {
    private String brokerName;

    private String brokerAddr;

    private String clusterName;

    private Long brokerId;

    private Long heartbeatTimeoutMillis;

    private boolean compressed;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public Long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(Long brokerId) {
        this.brokerId = brokerId;
    }

    public Long getHeartbeatTimeoutMillis() {
        return heartbeatTimeoutMillis;
    }

    public void setHeartbeatTimeoutMillis(Long heartbeatTimeoutMillis) {
        this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
    }

    public boolean isCompressed() {
        return compressed;
    }

    public void setCompressed(boolean compressed) {
        this.compressed = compressed;
    }
}
