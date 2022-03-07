package com.tsingj.sloth.store.log;

import com.tsingj.sloth.store.DataRecovery;
import com.tsingj.sloth.store.constants.LogConstants;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author yanghao
 */
public class TimeIndex implements DataRecovery {

    private static final Logger logger = LoggerFactory.getLogger(TimeIndex.class);

    public static final int INDEX_BYTES = 16;

    /**
     * 物理文件
     */
    private File file;

    /**
     * 文件读写channel
     */
    private FileChannel fileChannel;

    /**
     * fileLock
     */
    private final ReentrantLock readWriteLock;

    /**
     * offset index 数量
     */
    private long indexEntries;

    public TimeIndex(String logPath) throws FileNotFoundException {
        //init offsetIndex file operator
        this.file = new File(logPath + LogConstants.FileSuffix.TIMESTAMP_INDEX);
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();

        this.indexEntries = 0L;
        this.readWriteLock = new ReentrantLock();
    }

    //----------------------------------------------------------loadLogs--------------------------------------------------------------------

    @Override
    public void load() {
        long indexEntries = this.file.length() / TimeIndex.INDEX_BYTES;
        logger.info("load indexEntries {}", indexEntries);
        this.indexEntries = this.file.length() / TimeIndex.INDEX_BYTES;
    }

    public void addIndex(long key, long value) throws IOException {
        /*
         * add time index
         */
        ByteBuffer indexByteBuffer = ByteBuffer.allocate(TimeIndex.INDEX_BYTES);
        indexByteBuffer.putLong(key);
        indexByteBuffer.putLong(value);
        indexByteBuffer.flip();

        try {
            this.readWriteLock.lock();
            this.fileChannel.position(this.getWrotePosition());
            this.fileChannel.write(indexByteBuffer);
            this.incrementIndexEntries();
        } finally {
            this.readWriteLock.unlock();
        }

    }

    private long getWrotePosition() {
        return this.indexEntries * TimeIndex.INDEX_BYTES;
    }

    private void incrementIndexEntries() {
        this.indexEntries = this.indexEntries + 1;
    }


    public void flush() {
        try {
            this.fileChannel.force(true);
        } catch (IOException ignored) {
        }
    }

}
