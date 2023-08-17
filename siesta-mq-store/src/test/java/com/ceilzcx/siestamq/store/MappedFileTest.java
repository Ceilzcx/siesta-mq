package com.ceilzcx.siestamq.store;

import com.ceilzcx.siestamq.common.UtilAll;
import com.ceilzcx.siestamq.store.logfile.DefaultMappedFile;
import com.ceilzcx.siestamq.store.logfile.MappedFile;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author ceilzcx
 * @since 21/12/2022
 */
public class MappedFileTest {

    @Test
    public void testAppendMessage() {
        try {
            MappedFile mappedFile = new DefaultMappedFile("target/unit_test_store/MappedFileTest/000", 1000);
            String message = "hello world";
            mappedFile.appendMessage(message.getBytes(StandardCharsets.UTF_8));

            ByteBuffer byteBuffer = mappedFile.selectMappedBuffer(0);
            byte[] data = new byte[message.length()];
            byteBuffer.get(data);
            System.out.println(new String(data));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @After
    public void destroy() {
        File file = new File("target/unit_test_store");
        UtilAll.deleteFile(file);
    }
}
