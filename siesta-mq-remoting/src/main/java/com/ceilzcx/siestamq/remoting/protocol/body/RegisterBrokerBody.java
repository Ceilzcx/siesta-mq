package com.ceilzcx.siestamq.remoting.protocol.body;

import com.alibaba.fastjson.JSON;
import com.ceilzcx.siestamq.common.MixAll;
import com.ceilzcx.siestamq.common.TopicConfig;
import com.ceilzcx.siestamq.remoting.protocol.DataVersion;
import com.ceilzcx.siestamq.remoting.protocol.RemotingSerializable;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * @author ceilzcx
 * @since 20/12/2022
 */
@Slf4j
public class RegisterBrokerBody extends RemotingSerializable {
    private TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
    private List<String> filterServerList = new ArrayList<>();

    public byte[] encode(boolean compress) {

        if (!compress) {
            return super.encode();
        }
        long start = System.currentTimeMillis();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DeflaterOutputStream outputStream = new DeflaterOutputStream(byteArrayOutputStream, new Deflater(Deflater.BEST_COMPRESSION));
        DataVersion dataVersion = topicConfigSerializeWrapper.getDataVersion();
        ConcurrentMap<String, TopicConfig> topicConfigTable = cloneTopicConfigTable(topicConfigSerializeWrapper.getTopicConfigTable());
        assert topicConfigTable != null;
        try {
            byte[] buffer = dataVersion.encode();

            // write data version
            outputStream.write(convertIntToByteArray(buffer.length));
            outputStream.write(buffer);

            int topicNumber = topicConfigTable.size();

            // write number of topic configs
            outputStream.write(convertIntToByteArray(topicNumber));

            // write topic config entry one by one.
            for (ConcurrentMap.Entry<String, TopicConfig> next : topicConfigTable.entrySet()) {
                buffer = next.getValue().encode().getBytes(MixAll.DEFAULT_CHARSET);
                outputStream.write(convertIntToByteArray(buffer.length));
                outputStream.write(buffer);
            }

            buffer = JSON.toJSONString(filterServerList).getBytes(MixAll.DEFAULT_CHARSET);

            // write filter server list json length
            outputStream.write(convertIntToByteArray(buffer.length));

            // write filter server list json
            outputStream.write(buffer);

            outputStream.finish();
            long interval = System.currentTimeMillis() - start;
            if (interval > 50) {
                log.info("Compressing takes {}ms", interval);
            }
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            log.error("Failed to compress RegisterBrokerBody object", e);
        }

        return null;
    }

    public static RegisterBrokerBody decode(byte[] data, boolean compressed) throws IOException {
        if (!compressed) {
            return RegisterBrokerBody.decode(data, RegisterBrokerBody.class);
        }
        long start = System.currentTimeMillis();
        InflaterInputStream inflaterInputStream = new InflaterInputStream(new ByteArrayInputStream(data));
        int dataVersionLength = readInt(inflaterInputStream);
        byte[] dataVersionBytes = readBytes(inflaterInputStream, dataVersionLength);
        DataVersion dataVersion = DataVersion.decode(dataVersionBytes, DataVersion.class);

        RegisterBrokerBody registerBrokerBody = new RegisterBrokerBody();
        registerBrokerBody.getTopicConfigSerializeWrapper().setDataVersion(dataVersion);
        Map<String, TopicConfig> topicConfigTable = registerBrokerBody.getTopicConfigSerializeWrapper().getTopicConfigTable();

        int topicConfigNumber = readInt(inflaterInputStream);
        log.debug("{} topic configs to extract", topicConfigNumber);

        for (int i = 0; i < topicConfigNumber; i++) {
            int topicConfigJsonLength = readInt(inflaterInputStream);

            byte[] buffer = readBytes(inflaterInputStream, topicConfigJsonLength);
            TopicConfig topicConfig = new TopicConfig();
            String topicConfigJson = new String(buffer, MixAll.DEFAULT_CHARSET);
            topicConfig.decode(topicConfigJson);
            topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }

        int filterServerListJsonLength = readInt(inflaterInputStream);

        byte[] filterServerListBuffer = readBytes(inflaterInputStream, filterServerListJsonLength);
        String filterServerListJson = new String(filterServerListBuffer, MixAll.DEFAULT_CHARSET);
        List<String> filterServerList = new ArrayList<>();
        try {
            filterServerList = JSON.parseArray(filterServerListJson, String.class);
        } catch (Exception e) {
            log.error("Decompressing occur Exception {}", filterServerListJson);
        }

        registerBrokerBody.setFilterServerList(filterServerList);

        long interval = System.currentTimeMillis() - start;
        if (interval > 50) {
            log.info("Decompressing takes {}ms", interval);
        }
        return registerBrokerBody;
    }

    private static byte[] convertIntToByteArray(int n) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(n);
        return byteBuffer.array();
    }

    private static byte[] readBytes(InflaterInputStream inflaterInputStream, int length) throws IOException {
        byte[] buffer = new byte[length];
        int bytesRead = 0;
        while (bytesRead < length) {
            int len = inflaterInputStream.read(buffer, bytesRead, length - bytesRead);
            if (len == -1) {
                throw new IOException("End of compressed data has reached");
            } else {
                bytesRead += len;
            }
        }
        return buffer;
    }

    private static int readInt(InflaterInputStream inflaterInputStream) throws IOException {
        byte[] buffer = readBytes(inflaterInputStream, 4);
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
        return byteBuffer.getInt();
    }

    public TopicConfigSerializeWrapper getTopicConfigSerializeWrapper() {
        return topicConfigSerializeWrapper;
    }

    public void setTopicConfigSerializeWrapper(TopicConfigSerializeWrapper topicConfigSerializeWrapper) {
        this.topicConfigSerializeWrapper = topicConfigSerializeWrapper;
    }

    public List<String> getFilterServerList() {
        return filterServerList;
    }

    public void setFilterServerList(List<String> filterServerList) {
        this.filterServerList = filterServerList;
    }

    public static ConcurrentMap<String, TopicConfig> cloneTopicConfigTable(
            ConcurrentMap<String, TopicConfig> topicConfigConcurrentMap) {
        ConcurrentHashMap<String, TopicConfig> result = new ConcurrentHashMap<>();
        if (topicConfigConcurrentMap != null) {
            for (Map.Entry<String, TopicConfig> entry : topicConfigConcurrentMap.entrySet()) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;

    }
}
