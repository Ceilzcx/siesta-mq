package com.ceilzcx.siestamq.client;

import com.ceilzcx.siestamq.common.UtilAll;

/**
 * client端的配置项
 * @author ceilzcx
 * @since 3/4/2023
 */
public class ClientConfig {

    protected String namespace;

    private String clientIP;

    private String nameserverAddress;

    private String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");

    private String unitName;

    private int mqClientApiTimeout = 3000;

    public String getNameserverAddress() {
        return nameserverAddress;
    }

    public void setNameserverAddress(String nameserverAddress) {
        this.nameserverAddress = nameserverAddress;
    }

    public String getClientIP() {
        return clientIP;
    }

    public void setClientIP(String clientIP) {
        this.clientIP = clientIP;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public int getMqClientApiTimeout() {
        return mqClientApiTimeout;
    }

    public void setMqClientApiTimeout(int mqClientApiTimeout) {
        this.mqClientApiTimeout = mqClientApiTimeout;
    }

    public String buildClientInstance() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClientIP());

        sb.append("@");
        sb.append(this.getInstanceName());
        if (!UtilAll.isBlank(this.unitName)) {
            sb.append("@");
            sb.append(this.unitName);
        }

//        if (enableStreamRequestType) {
//            sb.append("@");
//            sb.append(RequestType.STREAM);
//        }

        return sb.toString();
    }

}
