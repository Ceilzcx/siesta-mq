package com.ceilzcx.siestamq.store.result;

/**
 * @author ceilzcx
 * @since 23/12/2022
 */
public enum PutMessageStatus {
    PUT_OK,
    FLUSH_DISK_TIMEOUT,
    FLUSH_SLAVE_TIMEOUT,
    SLAVE_NOT_AVAILABLE,
    SERVICE_NOT_AVAILABLE,
    CREATE_MAPPED_FILE_FAILED,
    MESSAGE_ILLEGAL,
    PROPERTIES_SIZE_EXCEEDED,
    OS_PAGE_CACHE_BUSY,
    UNKNOWN_ERROR,
    IN_SYNC_REPLICAS_NOT_ENOUGH,
    PUT_TO_REMOTE_BROKER_FAIL,
    LMQ_CONSUME_QUEUE_NUM_EXCEEDED,
    WHEEL_TIMER_FLOW_CONTROL,
    WHEEL_TIMER_MSG_ILLEGAL,
    WHEEL_TIMER_NOT_ENABLE
}
