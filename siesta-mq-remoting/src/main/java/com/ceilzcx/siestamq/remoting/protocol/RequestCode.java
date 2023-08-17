package com.ceilzcx.siestamq.remoting.protocol;

/**
 * @author ceilzcx
 * @since 15/12/2022
 */
public class RequestCode {

    public static final int PULL_MESSAGE = 11;

    public static final int UPDATE_AND_CREATE_TOPIC = 17;

    public static final int REGISTER_BROKER = 103;

    public static final int GET_ROUTE_DATA_BY_TOPIC = 105;

    public static final int REGISTER_TOPIC_IN_NAMESERVER = 217;

    public static final int LITE_PULL_MESSAGE = 361;
}
