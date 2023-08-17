package com.ceilzcx.siestamq.store.result;

import com.ceilzcx.siestamq.store.logfile.MappedFile;

import java.nio.ByteBuffer;

/**
 * @author ceilzcx
 * @since 23/12/2022
 */
public class SelectMappedBufferResult {
    private final long startOffset;

    private final ByteBuffer byteBuffer;

    private int size;

    protected MappedFile mappedFile;

    public SelectMappedBufferResult(long startOffset, ByteBuffer byteBuffer, int size, MappedFile mappedFile) {
        this.startOffset = startOffset;
        this.byteBuffer = byteBuffer;
        this.size = size;
        this.mappedFile = mappedFile;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public MappedFile getMappedFile() {
        return mappedFile;
    }

    public void setMappedFile(MappedFile mappedFile) {
        this.mappedFile = mappedFile;
    }
}
