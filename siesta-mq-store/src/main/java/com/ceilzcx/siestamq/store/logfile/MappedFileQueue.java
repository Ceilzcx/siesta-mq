package com.ceilzcx.siestamq.store.logfile;

import com.ceilzcx.siestamq.common.UtilAll;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author ceilzcx
 * @since 22/12/2022
 */
@Slf4j
public class MappedFileQueue {
    protected final String storePath;
    protected final int mappedFileSize;

    protected final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<>();

    public MappedFileQueue(final String storePath, int mappedFileSize) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
    }

    public boolean load() {
        File dir = new File(this.storePath);
        File[] ls = dir.listFiles();
        if (ls != null) {
            return doLoad(Arrays.asList(ls));
        }
        return true;
    }

    public boolean doLoad(List<File> files) {
        // 保证mappedFiles的顺序
        files.sort(Comparator.comparing(File::getName));

        for (File file : files) {
            if (file.isDirectory()) {
                continue;
            }
            if (file.length() != this.mappedFileSize) {
                return false;
            }

            try {
                MappedFile mappedFile = new DefaultMappedFile(file.getPath(), mappedFileSize);

                mappedFile.setWrotePosition(this.mappedFileSize);
                mappedFile.setFlushedPosition(this.mappedFileSize);
                mappedFile.setCommittedPosition(this.mappedFileSize);
                this.mappedFiles.add(mappedFile);
            } catch (IOException e) {
                return false;
            }
        }

        return true;
    }

    public MappedFile getFirstMappedFile() {
        if (!this.mappedFiles.isEmpty()) {
            try {
                return this.mappedFiles.get(0);
            } catch (Exception e) {
                // 猜测多线程情况下出现mappedFiles被删除 IndexOutOfBoundsException 的情况
            }
        }
        return null;
    }

    public MappedFile getLastMappedFile() {
        // 不知道为什么源码要把list转成array后再获取
        return this.mappedFiles.isEmpty() ? null : this.mappedFiles.get(this.mappedFiles.size() - 1);
    }

    public MappedFile getLastMappedFile(final long startOffset) {
        return this.getLastMappedFile(startOffset, true);
    }

    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        MappedFile lastMappedFile = this.getLastMappedFile();
        long createOffset = -1;
        if (lastMappedFile == null) {
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        } else if (lastMappedFile.isFull()) {
            createOffset = lastMappedFile.getFileFromOffset() + this.mappedFileSize;
        }
        if (createOffset != -1 && needCreate) {
            lastMappedFile = this.tryCreateMappedFile(createOffset);
        }
        return lastMappedFile;
    }

    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstIfNotFound) {
        MappedFile firstMappedFile = this.getFirstMappedFile();
        MappedFile lastMappedFile = this.getLastMappedFile();
        if (firstMappedFile != null && lastMappedFile != null) {
            // 判断offset是否在mappedFiles之间, 大于等于第一个文件的最小偏移, 小于等于最后一个文件的最大偏移
            if (firstMappedFile.getFileFromOffset() >= offset && lastMappedFile.getFileFromOffset() + this.mappedFileSize <= offset) {
                int index = (int) ((offset - firstMappedFile.getFileFromOffset()) / this.mappedFileSize);
                MappedFile targetMappedFile = null;
                try {
                    targetMappedFile = this.mappedFiles.get(index);
                } catch (Exception ignore) {
                }
                if (targetMappedFile != null && offset >= targetMappedFile.getFileFromOffset()
                        && offset <= targetMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    return targetMappedFile;
                }

                // 通过下标查询存在问题, 遍历查询
                for (MappedFile mappedFile: this.mappedFiles) {
                    if (offset >= mappedFile.getFileFromOffset() && offset <= mappedFile.getFileFromOffset() + this.mappedFileSize) {
                        return mappedFile;
                    }
                }

                if (returnFirstIfNotFound) {
                    return firstMappedFile;
                }
            } else {
                log.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
                        offset,
                        firstMappedFile.getFileFromOffset(),
                        lastMappedFile.getFileFromOffset() + this.mappedFileSize,
                        this.mappedFileSize,
                        this.mappedFiles.size());
            }
        }
        return null;
    }

    public MappedFile tryCreateMappedFile(long createOffset) {
        String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
        String nextNextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset
                + this.mappedFileSize);
        return doCreateMappedFile(nextFilePath, nextNextFilePath);
    }

    // todo 目前没理解nextNextFilePath的使用意义
    public MappedFile doCreateMappedFile(String nextFilePath, String nextNextFilePath) {

        // 只创建nextFilePath
        try {
            MappedFile mappedFile = new DefaultMappedFile(nextFilePath, this.mappedFileSize);
            this.mappedFiles.add(mappedFile);
            return mappedFile;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }
}
