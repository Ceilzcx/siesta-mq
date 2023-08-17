package com.ceilzcx.siestamq.store.logfile;

import com.ceilzcx.siestamq.store.result.SelectMappedBufferResult;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author ceilzcx
 * @since 21/12/2022
 */
public interface MappedFile {

    void init(String fileName, int fileSize) throws IOException;

    String getFileName();

    int getFileSize();

    long getFileFromOffset();

    FileChannel getFileChannel();

    boolean isFull();

    boolean isAvailable();

    boolean appendMessage(byte[] data);

    boolean appendMessage(ByteBuffer data);

    boolean appendMessage(byte[] data, int offset, int length);

    SelectMappedBufferResult selectMappedBuffer(int pos);

    int getReadPosition();

    void setWrotePosition(int wrotePosition);

    void setFlushedPosition(int flushedPosition);

    void setCommittedPosition(int committedPosition);
}
