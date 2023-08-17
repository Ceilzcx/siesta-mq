package com.ceilzcx.siestamq.common.topic;

import java.util.HashSet;
import java.util.Set;

/**
 * @author ceilzcx
 * @since 19/12/2022
 */
public class TopicValidator {
    // 默认的topic, 用于查看broker是否可用
    public static final String AUTO_CREATE_TOPIC_KEY_TOPIC = "TBW102";

    public static final String SYSTEM_TOPIC_PREFIX = "rmq_sys_";

    private static final int TOPIC_MAX_LENGTH = 127;

    private static final Set<String> SYSTEM_TOPIC_SET = new HashSet<>();

    static {
        SYSTEM_TOPIC_SET.add(AUTO_CREATE_TOPIC_KEY_TOPIC);
    }

    public static ValidateTopicResult validateTopic(String topic) {
        if (topic == null || topic.isEmpty()) {
            return new ValidateTopicResult(false, "The specified topic is blank.");
        }

        if (topic.length() > TOPIC_MAX_LENGTH) {
            return new ValidateTopicResult(false, "The specified topic is longer than topic max length.");
        }

        return new ValidateTopicResult(true, "");
    }

    public static boolean isSystemTopic(String topic) {
        return SYSTEM_TOPIC_SET.contains(topic) || topic.startsWith(SYSTEM_TOPIC_PREFIX);
    }

    public static class ValidateTopicResult {
        private final boolean valid;
        private final String remark;

        public ValidateTopicResult(boolean valid, String remark) {
            this.valid = valid;
            this.remark = remark;
        }

        public boolean isValid() {
            return valid;
        }

        public String getRemark() {
            return remark;
        }
    }
}
