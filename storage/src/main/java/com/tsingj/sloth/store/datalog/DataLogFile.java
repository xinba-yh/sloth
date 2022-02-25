package com.tsingj.sloth.store.datalog;

import com.tsingj.sloth.store.Result;
import com.tsingj.sloth.store.Results;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

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


    private long maxFileSize;

    //log物理文件
    private File logFile;

    private BufferedOutputStream logWriter;

    //offsetIndex物理文件
    private File offsetIndexFile;

    private BufferedOutputStream offsetIndexWriter;

    //timeIndex物理文件
    private File timeIndexFile;

    private BufferedOutputStream timeIndexWriter;


    public DataLogFile() {

    }

    public DataLogFile(String logPath, String offsetIndexPath, String timeIndexPath, long startOffset, int maxFileSize) throws FileNotFoundException {
        this.firstInit(logPath, offsetIndexPath, timeIndexPath, startOffset, maxFileSize);
    }

    private void firstInit(String logPath, String offsetIndexPath, String timeIndexPath, long startOffset, int maxFileSize) throws FileNotFoundException {
        logFile = new File(logPath);
        this.logWriter = new BufferedOutputStream(new FileOutputStream(logFile, true));
        offsetIndexFile = new File(offsetIndexPath);
        this.offsetIndexWriter = new BufferedOutputStream(new FileOutputStream(offsetIndexFile, true));
        timeIndexFile = new File(timeIndexPath);
        this.timeIndexWriter = new BufferedOutputStream(new FileOutputStream(timeIndexFile, true));
        log.info("init logFIle:{}，offsetIndexFile:{}，timeIndexFile:{}.", logPath, offsetIndexPath, timeIndexPath);

        this.maxFileSize = maxFileSize;
        this.currentOffset = startOffset;
        this.fileFromOffset = startOffset;
        this.wrotePosition = 0;
    }

    public boolean isFull() {
        return FileUtils.sizeOf(logFile) >= maxFileSize;
    }

    public boolean isFull(int needLen) {
        return (FileUtils.sizeOf(logFile) + needLen) > maxFileSize;
    }

//    public long getCurrentOffset() {
//        return this.currentOffset;
//    }

    public Result doAppend(byte[] data, long offset, long storeTimestamp) {
        try {
            /*
             * append log
             */
            this.logWriter.write(data);

            // TODO: 2022/2/25 改为稀疏索引
            /*
             * append offset index
             */
            //根据append过的字节数，计算新的position
            this.wrotePosition = this.wrotePosition + data.length;
            ByteBuffer indexByteBuffer = ByteBuffer.allocate(16);
            indexByteBuffer.putLong(offset);
            indexByteBuffer.putLong(this.wrotePosition);
            offsetIndexWriter.write(indexByteBuffer.array());

            // TODO: 2022/2/25 改为稀疏索引
            /*
             * append time index
             */
            ByteBuffer timeIndexByteBuffer = ByteBuffer.allocate(16);
            timeIndexByteBuffer.putLong(storeTimestamp);
            timeIndexByteBuffer.putLong(offset);
            this.timeIndexWriter.write(timeIndexByteBuffer.array());

            return Results.success();
        } catch (IOException e) {
            log.error("log append fail!", e);
            return Results.failure("log append fail!" + e.getMessage());
        }
    }

    public long incrementOffsetAndGet() {
        this.currentOffset = this.currentOffset + 1;
        return currentOffset;
    }
}
