package com.ceilzcx.siestamq.store.config;

/**
 * @author ceilzcx
 * @since 23/12/2022
 */
public class MessageStoreConfig {
    private String commitLogStorePath;

    private int CommitLogMappedFileSize = 1024 * 1024 * 1024;

    public int getCommitLogMappedFileSize() {
        return CommitLogMappedFileSize;
    }

    public void setCommitLogMappedFileSize(int commitLogMappedFileSize) {
        CommitLogMappedFileSize = commitLogMappedFileSize;
    }

    public String getCommitLogStorePath() {
        return commitLogStorePath;
    }

    public void setCommitLogStorePath(String commitLogStorePath) {
        this.commitLogStorePath = commitLogStorePath;
    }
}
