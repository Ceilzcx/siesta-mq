package com.ceilzcx.siestamq.store.logfile;

import com.ceilzcx.siestamq.store.DefaultMessageStore;
import com.ceilzcx.siestamq.store.result.PutMessageResult;
import com.ceilzcx.siestamq.store.result.SelectMappedBufferResult;

import java.util.concurrent.CompletableFuture;

/**
 * @author ceilzcx
 * @since 23/12/2022
 */
public class CommitLog {
    protected final MappedFileQueue mappedFileQueue;
    protected final DefaultMessageStore defaultMessageStore;

    public CommitLog(final DefaultMessageStore messageStore) {
        this.defaultMessageStore = messageStore;

        String storePath = messageStore.getMessageStoreConfig().getCommitLogStorePath();
        {
            this.mappedFileQueue = new MappedFileQueue(storePath, messageStore.getMessageStoreConfig().getCommitLogMappedFileSize());
        }

    }

    public boolean load() {
        // todo check self?
        return this.mappedFileQueue.load();
    }

    public SelectMappedBufferResult getData(final long offset) {
        return this.getData(offset, offset == 0);
    }

    public SelectMappedBufferResult getData(final long offset, final boolean returnFirstIfNotFound) {
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, returnFirstIfNotFound);
        if (mappedFile != null) {
            int pos = (int) (offset - mappedFile.getFileFromOffset());
            // 源码使用余的方式, 个人感觉不如直接减
            // int pos = (int) (offset % mappedFileSize);
            return mappedFile.selectMappedBuffer(pos);
        }
        return null;
    }

//    public CompletableFuture<PutMessageResult> asyncPutMessage(final ) {
//
//    }
}
