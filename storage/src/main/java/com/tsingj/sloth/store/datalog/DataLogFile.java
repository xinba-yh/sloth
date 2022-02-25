package com.tsingj.sloth.store.datalog;

import com.tsingj.sloth.store.Result;
import com.tsingj.sloth.store.Results;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * @author yanghao
 */
@Slf4j
public class DataLogFile {

    /**
     * 文件开始offset
     */
    private long fileFromOffset;

    /**
     * 当前offset
     */

    private long currentOffset;

    /**
     * 写入position
     */
    private long wrotePosition;


    /**
     * 当前文件最大值
     */
    private long maxFileSize;

    /**
     * log物理文件
     */
    private File logFile;

    private BufferedOutputStream logWriter;

    /**
     * offsetIndex物理文件
     */
    private File offsetIndexFile;

    private BufferedOutputStream offsetIndexWriter;

    /**
     * timeIndex物理文件
     */
    private File timeIndexFile;

    private BufferedOutputStream timeIndexWriter;

    /**
     * 记录上一次索引项添加后, 多少bytes的新消息存进来
     */
    private int bytesSinceLastIndexAppend;

    /**
     * 索引项刷追加bytes间隔
     */
    private int logIndexIntervalBytes;


    public DataLogFile(String logPath, String offsetIndexPath, String timeIndexPath, long startOffset, int maxFileSize, int logIndexIntervalBytes) throws FileNotFoundException {
        this.firstInit(logPath, offsetIndexPath, timeIndexPath, startOffset, maxFileSize, logIndexIntervalBytes);
    }

    private void firstInit(String logPath, String offsetIndexPath, String timeIndexPath, long startOffset, int maxFileSize, int logIndexIntervalBytes) throws FileNotFoundException {
        logFile = new File(logPath);
        this.logWriter = new BufferedOutputStream(new FileOutputStream(logFile, true));
        offsetIndexFile = new File(offsetIndexPath);
        this.offsetIndexWriter = new BufferedOutputStream(new FileOutputStream(offsetIndexFile, true));
        timeIndexFile = new File(timeIndexPath);
        this.timeIndexWriter = new BufferedOutputStream(new FileOutputStream(timeIndexFile, true));
        log.info("init logFile:{}，offsetIndexFile:{}，timeIndexFile:{}.", logPath, offsetIndexPath, timeIndexPath);

        this.maxFileSize = maxFileSize;
        this.currentOffset = startOffset;
        this.fileFromOffset = startOffset;
        this.wrotePosition = 0;
        this.logIndexIntervalBytes = logIndexIntervalBytes;
    }

    public boolean isFull() {
        return wrotePosition >= maxFileSize;
    }

    public Result doAppend(byte[] data, long offset, long storeTimestamp) {
        try {
            /*
             * 1、append log
             */
            this.logWriter.write(data);
            //根据append过的字节数，计算新的position
            long newWrotePosition = incrementWrotePosition(data.length);
            // TODO: 2022/2/25 抽象至offsetIndex和timestampIndex
            //记录上一次index插入后，新存入的消息
            long recordBytes = this.recordBytesSinceLastIndexAppend(data.length);
            if (recordBytes >= this.logIndexIntervalBytes) {
                /*
                 * 2、append offset index
                 */
                ByteBuffer indexByteBuffer = ByteBuffer.allocate(16);
                indexByteBuffer.putLong(offset);
                indexByteBuffer.putLong(newWrotePosition);
                offsetIndexWriter.write(indexByteBuffer.array());
                /*
                 * 3、append time index
                 */
                ByteBuffer timeIndexByteBuffer = ByteBuffer.allocate(16);
                timeIndexByteBuffer.putLong(storeTimestamp);
                timeIndexByteBuffer.putLong(offset);
                this.timeIndexWriter.write(timeIndexByteBuffer.array());
                this.resetBytesSinceLastIndexAppend();
            }

            return Results.success();
        } catch (IOException e) {
            log.error("log append fail!", e);
            return Results.failure("log append fail!" + e.getMessage());
        }
    }

    private long incrementWrotePosition(int dataLen) {
        this.wrotePosition = this.wrotePosition + dataLen;
        return this.wrotePosition;
    }

    private long recordBytesSinceLastIndexAppend(int dataLen) {
        this.bytesSinceLastIndexAppend = this.bytesSinceLastIndexAppend + dataLen;
        return this.bytesSinceLastIndexAppend;
    }

    private void resetBytesSinceLastIndexAppend() {
        this.bytesSinceLastIndexAppend = 0;
    }

    public long incrementOffsetAndGet() {
        this.currentOffset = this.currentOffset + 1;
        return currentOffset;
    }
}
