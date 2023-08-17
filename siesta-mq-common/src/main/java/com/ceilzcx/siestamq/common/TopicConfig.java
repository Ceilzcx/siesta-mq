package com.ceilzcx.siestamq.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.ceilzcx.siestamq.common.constant.PermName;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ceilzcx
 * @since 19/12/2022
 */
public class TopicConfig {
    // attributes的分隔符
    private static final String SEPARATOR = " ";

    private static final int DEFAULT_READ_QUEUE_NUMS = 16;
    private static final int DEFAULT_WRITE_QUEUE_NUMS = 16;
    private static final TypeReference<Map<String, String>> ATTRIBUTES_TYPE_REFERENCE =
            new TypeReference<Map<String, String>>() {};

    private String topicName;
    private int readQueueNums = DEFAULT_READ_QUEUE_NUMS;
    private int writeQueueNums = DEFAULT_WRITE_QUEUE_NUMS;

    private int perm = PermName.PERM_READ | PermName.PERM_WRITE;

    private TopicFilterType topicFilterType = TopicFilterType.SINGLE_TAG;
    // Field attributes should not have ' ' char in key or value, otherwise will lead to decode failure.
    private Map<String, String> attributes = new HashMap<>();

    public TopicConfig() {
    }

    public TopicConfig(String topicName) {
        this.topicName = topicName;
    }

    public TopicConfig(String topicName, int readQueueNums, int writeQueueNums) {
        this.topicName = topicName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
    }

    public TopicConfig(String topicName, int readQueueNums, int writeQueueNums, int perm) {
        this.topicName = topicName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
        this.perm = perm;
    }


    public String encode() {
        StringBuilder sb = new StringBuilder();
        //[0]
        sb.append(this.topicName);
        sb.append(SEPARATOR);
        //[1]
        sb.append(this.readQueueNums);
        sb.append(SEPARATOR);
        //[2]
        sb.append(this.writeQueueNums);
        sb.append(SEPARATOR);
        //[3]
        sb.append(this.perm);
        sb.append(SEPARATOR);
        //[4]
        sb.append(this.topicFilterType);
        sb.append(SEPARATOR);
        //[5]
        if (attributes != null) {
            sb.append(JSON.toJSONString(attributes));
        }

        return sb.toString();
    }


    public boolean decode(final String in) {
        String[] strs = in.split(SEPARATOR);
        if (strs.length >= 5) {
            this.topicName = strs[0];

            this.readQueueNums = Integer.parseInt(strs[1]);

            this.writeQueueNums = Integer.parseInt(strs[2]);

            this.perm = Integer.parseInt(strs[3]);

            this.topicFilterType = TopicFilterType.valueOf(strs[4]);

            if (strs.length >= 6) {
                try {
                    this.attributes = JSON.parseObject(strs[5], ATTRIBUTES_TYPE_REFERENCE.getType());
                } catch (Exception e) {
                    // ignore exception when parse failed, cause map's key/value can have ' ' char.
                }
            }

            return true;
        }

        return false;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getReadQueueNums() {
        return readQueueNums;
    }

    public void setReadQueueNums(int readQueueNums) {
        this.readQueueNums = readQueueNums;
    }

    public int getWriteQueueNums() {
        return writeQueueNums;
    }

    public void setWriteQueueNums(int writeQueueNums) {
        this.writeQueueNums = writeQueueNums;
    }

    public int getPerm() {
        return perm;
    }

    public void setPerm(int perm) {
        this.perm = perm;
    }
}
