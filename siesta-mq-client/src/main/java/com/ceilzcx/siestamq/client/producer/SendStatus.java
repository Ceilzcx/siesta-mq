package com.ceilzcx.siestamq.client.producer;

/**
 * @author ceilzcx
 * @since 3/4/2023
 */
public enum SendStatus {
    SEND_OK,
    FLUSH_DISK_TIMEOUT,
    FLUSH_SLAVE_TIMEOUT,
    SLAVE_NOT_AVAILABLE,
}
