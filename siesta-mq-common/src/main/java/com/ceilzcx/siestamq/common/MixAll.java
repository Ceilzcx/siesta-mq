package com.ceilzcx.siestamq.common;

/**
 * @author ceilzcx
 * @since 20/12/2022
 */
public class MixAll {

    public static final String DEFAULT_CHARSET = "UTF-8";

    public static final String CLIENT_INNER_PRODUCER_GROUP = "CLIENT_INNER_PRODUCER";

    public static final long MASTER_ID = 0L;

    public static final String METADATA_SCOPE_GLOBAL = "__global__";

    // 消息重试时的topic前缀
    public static final String RETRY_GROUP_TOPIC_PREFIX = "%RETRY%";
}
