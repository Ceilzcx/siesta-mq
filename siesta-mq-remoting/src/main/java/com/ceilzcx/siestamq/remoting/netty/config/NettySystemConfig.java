package com.ceilzcx.siestamq.remoting.netty.config;

/**
 * @author ceilzcx
 * @since 14/12/2022
 */
public class NettySystemConfig {
    public static final String COM_ROCKETMQ_REMOTING_CLIENT_ASYNC_SEMAPHORE_VALUE =
            "com.rocketmq.remoting.clientAsyncSemaphoreValue";
    public static final String COM_ROCKETMQ_REMOTING_CLIENT_ONEWAY_SEMAPHORE_VALUE =
            "com.rocketmq.remoting.clientOnewaySemaphoreValue";
    public static final String COM_ROCKETMQ_REMOTING_CLIENT_WORKER_SIZE =
            "com.rocketmq.remoting.client.worker.size";
    public static final String COM_ROCKETMQ_REMOTING_CLIENT_CONNECT_TIMEOUT =
            "com.rocketmq.remoting.client.connect.timeout";

    public static final int CLIENT_ASYNC_SEMAPHORE_VALUE =
            Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_CLIENT_ASYNC_SEMAPHORE_VALUE, "65535"));
    public static final int CLIENT_ONEWAY_SEMAPHORE_VALUE =
            Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_CLIENT_ONEWAY_SEMAPHORE_VALUE, "65535"));
    public static final int CLIENT_WORKER_SIZE =
            Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_CLIENT_WORKER_SIZE, "4"));
    public static int CONNECT_TIMEOUT_MILLIS =
            Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_CLIENT_CONNECT_TIMEOUT, "3000"));
}
