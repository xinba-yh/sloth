package com.tsingj.sloth.store.log;

import com.tsingj.sloth.store.DataLogConstants;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author yanghao
 */
public class TimeIndex {

    private static final Logger logger = LoggerFactory.getLogger(TimeIndex.class);

    /**
     * 物理文件
     */
    private File file;

    /**
     * 文件读写channel
     */
    private FileChannel fileChannel;

    /**
     * offset index 数量
     */
    private long indexEntries;

    public TimeIndex(String logPath) throws FileNotFoundException {
        //init offsetIndex file operator
        file = new File(logPath + DataLogConstants.FileSuffix.TIMESTAMP_INDEX);
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();

        this.indexEntries = 0L;
    }

    public void addIndex(long key, long value) throws IOException {
        /*
         * add time index
         */
        ByteBuffer indexByteBuffer = ByteBuffer.allocate(DataLogConstants.INDEX_BYTES);
        indexByteBuffer.putLong(key);
        indexByteBuffer.putLong(value);
        this.fileChannel.position(this.getWrotePosition());
        this.fileChannel.write(indexByteBuffer);
        this.incrementIndexEntries();
    }

    private long getWrotePosition() {
        return this.indexEntries * DataLogConstants.INDEX_BYTES;
    }

    private void incrementIndexEntries() {
        this.indexEntries = this.indexEntries + 1;
    }

    public long getFileSize() {
        return FileUtils.sizeOf(file);
    }

    public void loadLogs() {
        long indexEntries = file.length() / DataLogConstants.INDEX_BYTES;
        logger.info("load indexEntries {}", indexEntries);
        this.indexEntries = file.length() / DataLogConstants.INDEX_BYTES;
    }

    public void flush() {
        try {
            this.fileChannel.force(true);
        } catch (IOException ignored) {
        }
    }

}
