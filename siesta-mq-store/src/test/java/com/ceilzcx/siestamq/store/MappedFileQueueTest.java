package com.ceilzcx.siestamq.store;

import com.ceilzcx.siestamq.store.logfile.MappedFile;
import com.ceilzcx.siestamq.store.logfile.MappedFileQueue;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;

/**
 * @author ceilzcx
 * @since 22/12/2022
 */
public class MappedFileQueueTest {
    private String storePath = System.getProperty("java.io.tmpdir") + File.separator + "unit_test_store";

    @Test
    public void testGetLastMappedFile() {
        final String fixedMsg = "0123456789abcdef";

        MappedFileQueue mappedFileQueue = new MappedFileQueue(storePath + File.separator + "a/", 1024);
        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile(0);
            mappedFile.appendMessage(fixedMsg.getBytes(StandardCharsets.UTF_8));
        }
    }

}
