package com.ceilzcx.siestamq.client.exception;

/**
 * @author ceilzcx
 * @since 28/9/2023
 */
public class MQBrokerException extends Exception {
    private static final long serialVersionUID = 5975020272601250368L;
    private final int responseCode;
    private final String errorMessage;
    private final String brokerAddr;

    MQBrokerException() {
        this.responseCode = 0;
        this.errorMessage = null;
        this.brokerAddr = null;
    }

    public MQBrokerException(int responseCode, String errorMessage) {
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
        this.brokerAddr = null;
    }

    public MQBrokerException(int responseCode, String errorMessage, String brokerAddr) {
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
        this.brokerAddr = brokerAddr;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }
}
