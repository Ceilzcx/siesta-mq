package com.ceilzcx.siestamq.remoting.protocol;

/**
 * @author ceilzcx
 * @since 16/8/2023
 */
public class ResponseCode extends RemotingSysResponseCode {

    public static final int FLUSH_DISK_TIMEOUT = 10;

    public static final int SLAVE_NOT_AVAILABLE = 11;

    public static final int FLUSH_SLAVE_TIMEOUT = 12;

    public static final int TOPIC_NOT_EXIST = 17;

}
