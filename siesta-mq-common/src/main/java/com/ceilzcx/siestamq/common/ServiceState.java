package com.ceilzcx.siestamq.common;

/**
 * @author ceilzcx
 * @since 3/4/2023
 */
public enum ServiceState {
    /**
     * Service just created,not start
     */
    CREATE_JUST,
    /**
     * Service Running
     */
    RUNNING,
    /**
     * Service shutdown
     */
    SHUTDOWN_ALREADY,
    /**
     * Service Start failure
     */
    START_FAILED;
}
