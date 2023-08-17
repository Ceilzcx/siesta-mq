package com.ceilzcx.siestamq.store.logfile;

import com.ceilzcx.siestamq.common.UtilAll;
import com.ceilzcx.siestamq.store.result.SelectMappedBufferResult;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * @author ceilzcx
 * @since 21/12/2022
 */
public class DefaultMappedFile implements MappedFile {

    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> WROTE_POSITION_UPDATER;
    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> COMMITTED_POSITION_UPDATER;
    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> FLUSHED_POSITION_UPDATER;

    protected volatile int wrotePosition;
    protected volatile int committedPosition;
    protected volatile int flushedPosition;

    protected String fileName;
    protected int fileSize;
    protected long fileFromOffset;
    protected File file;
    protected ByteBuffer writeBuffer;
    protected FileChannel fileChannel;
    protected MappedByteBuffer mappedByteBuffer;

    static {
        WROTE_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "wrotePosition");
        COMMITTED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "committedPosition");
        FLUSHED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "flushedPosition");
    }

    public DefaultMappedFile() {
    }

    public DefaultMappedFile(final String fileName, final int fileSize) throws IOException {
        this.init(fileName, fileSize);
    }

    @Override
    public void init(String fileName, int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        UtilAll.ensureDirOK(this.file.getParent());

        RandomAccessFile accessFile = null;
        try {
            accessFile = new RandomAccessFile(this.file, "rw");
            this.fileChannel = accessFile.getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            ok = true;
        } finally {
            if (!ok) {
                if (this.fileChannel != null) {
                    this.fileChannel.close();
                }
                if (accessFile != null) {
                    accessFile.close();
                }
            }
        }
    }

    @Override
    public String getFileName() {
        return null;
    }

    @Override
    public int getFileSize() {
        return this.fileSize;
    }

    @Override
    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    @Override
    public FileChannel getFileChannel() {
        return this.fileChannel;
    }

    @Override
    public boolean isFull() {
        return this.fileSize == WROTE_POSITION_UPDATER.get(this);
    }

    @Override
    public boolean isAvailable() {
        return false;
    }

    @Override
    public boolean appendMessage(final byte[] data) {
        return this.appendMessage(data, 0, data.length);
    }

    @Override
    public boolean appendMessage(ByteBuffer data) {
        return false;
    }

    @Override
    public boolean appendMessage(final byte[] data, int offset, int length) {
        int currentPosition = WROTE_POSITION_UPDATER.get(this);
        if (currentPosition + data.length <= this.fileSize) {
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(currentPosition);
            byteBuffer.put(data, offset, length);
            WROTE_POSITION_UPDATER.addAndGet(this, length);
            return true;
        }
        return false;
    }

    @Override
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = this.getReadPosition();
        if (pos < readPosition && pos >= 0) {
            int size = readPosition - pos;
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(pos);
            ByteBuffer newByteBuffer = byteBuffer.slice();
            newByteBuffer.limit(size);
            return new SelectMappedBufferResult(this.fileFromOffset + pos, newByteBuffer, size, this);
        }
        return null;
    }

    @Override
    public int getReadPosition() {
        return this.writeBuffer == null ? WROTE_POSITION_UPDATER.get(this) : COMMITTED_POSITION_UPDATER.get(this);
    }

    @Override
    public void setWrotePosition(int wrotePosition) {
        this.wrotePosition = wrotePosition;
    }

    @Override
    public void setFlushedPosition(int flushedPosition) {
        this.flushedPosition = flushedPosition;
    }

    @Override
    public void setCommittedPosition(int committedPosition) {
        this.committedPosition = committedPosition;
    }
}
