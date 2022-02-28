package com.tsingj.sloth.store.log;

import com.tsingj.sloth.store.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author yanghao
 */
@Slf4j
public class LogSegment {

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

    private FileChannel logReader;

    private BufferedOutputStream logWriter;

    /**
     * log offsetIndex
     */
    private OffsetIndex offsetIndex;

    /**
     * log timeIndex
     */
    private TimeIndex timeIndex;


    /**
     * 记录上一次索引项添加后, 多少bytes的新消息存进来
     */
    private int bytesSinceLastIndexAppend;

    /**
     * 索引项刷追加bytes间隔
     */
    private int logIndexIntervalBytes;


    public LogSegment(String logPath, long startOffset, int maxFileSize, int logIndexIntervalBytes) throws FileNotFoundException {
        this.firstInit(logPath, startOffset, maxFileSize, logIndexIntervalBytes);
    }

    private void firstInit(String logPath, long startOffset, int maxFileSize, int logIndexIntervalBytes) throws FileNotFoundException {
        //init log file operator
        logFile = new File(logPath + DataLogConstants.FileSuffix.LOG);
        this.logWriter = new BufferedOutputStream(new FileOutputStream(logFile, true));
        this.logReader = new RandomAccessFile(logFile, "r").getChannel();
        //init offsetIndex
        this.offsetIndex = new OffsetIndex(logPath);
        //init timeIndex
        this.timeIndex = new TimeIndex(logPath);
        log.info("init logFile success... \n logFile:{} \n offsetIndexFile:{} \n timeIndexFile:{}.", logPath + DataLogConstants.FileSuffix.LOG, logPath + DataLogConstants.FileSuffix.OFFSET_INDEX, logPath + DataLogConstants.FileSuffix.TIMESTAMP_INDEX);

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
            this.logWriter.flush();

            //记录上一次index插入后，新存入的消息
            long recordBytes = this.recordBytesSinceLastIndexAppend(data.length);
            if (offset == 0 || recordBytes >= this.logIndexIntervalBytes) {
                /*
                 * 2、append offset index
                 */
                this.offsetIndex.addIndex(offset, offset == 0 ? 0L : this.wrotePosition);
                /*
                 * 3、append time index
                 */
                this.timeIndex.addIndex(storeTimestamp, offset);

                //reset index append bytes.
                this.resetBytesSinceLastIndexAppend();
            }

            //根据append过的字节数，计算新的position
            incrementWrotePosition(data.length);

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

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    public Result<ByteBuffer> getMessage(long offset) {
        //1、lookup logPosition slot range
        Result<IndexEntry.OffsetPosition> lookUpResult = this.offsetIndex.lookUp(offset);
        if (lookUpResult.failure()) {
            return Results.failure(lookUpResult.getMsg());
        }
        //2、slot logFile position find real position
        // TODO: 2022/2/25 add endPosition. maxMessageSize+slotSize || lookUp返回下一个索引的下标
        IndexEntry.OffsetPosition indexEntry = lookUpResult.getData();
        Long startPosition =indexEntry.getPosition();
        Long endPosition = FileUtils.sizeOf(logFile);
        Result<Long> logPositionResult = this.findLogPositionSlotRange(offset, startPosition, endPosition);
        //3、get log bytes
        if (logPositionResult.failure()) {
            return Results.failure(logPositionResult.getMsg());
        }
        Long position = logPositionResult.getData();
        return this.getMessageByPosition(position);
    }

    private Result<ByteBuffer> getMessageByPosition(Long position) {
        try {
            logReader.position(position);
            ByteBuffer headerByteBuffer = ByteBuffer.allocate(DataLogConstants.MessageKeyBytes.LOG_OVERHEAD);
            logReader.read(headerByteBuffer);
            headerByteBuffer.rewind();

            long offset = headerByteBuffer.getLong();
            int storeSize = headerByteBuffer.getInt();
            ByteBuffer storeByteBuffer = ByteBuffer.allocate(storeSize);
            logReader.read(storeByteBuffer);
            return Results.success(storeByteBuffer);

//            ByteBuffer messageByteBuffer= ByteBuffer.allocate(DataLogConstants.MessageKeyBytes.LOG_OVERHEAD + storeSize);
//            messageByteBuffer.put(headerByteBuffer.array());
//            messageByteBuffer.put(storeByteBuffer.array());
//            return Results.success(messageByteBuffer.array());
        } catch (IOException e) {
            log.error("get message by position {} , IO operation fail!", position, e);
            return Results.failure("get message by position {} IO operation fail!");
        }
    }

    private Result<Long> findLogPositionSlotRange(long searchOffset, Long startPosition, Long endPosition) {
        Long logPosition = null;
        long position = startPosition;
        while (position < endPosition) {
            //查询消息体
            try {
                logReader.position(position);
                ByteBuffer headerByteBuffer = ByteBuffer.allocate(DataLogConstants.MessageKeyBytes.LOG_OVERHEAD);
                logReader.read(headerByteBuffer);
                headerByteBuffer.rewind();
                long offset = headerByteBuffer.getLong();
                int storeSize = headerByteBuffer.getInt();
                if (searchOffset == offset) {
                    logPosition = position;
                    break;
                } else {
                    position = position + (DataLogConstants.MessageKeyBytes.OFFSET + storeSize);
                }
            } catch (IOException e) {
                log.error("find offset:{} IO operation fail!", searchOffset, e);
                return Results.failure("find offset:" + searchOffset + " IO operation fail!");
            }
        }

        if (logPosition == null) {
            log.warn("file:{} , position start:{} end:{}, find offset:{} fail!", logFile.getName(), startPosition, endPosition, searchOffset);
            return Results.failure("offset " + searchOffset + " find fail!");
        }
        return Results.success(logPosition);
    }



}
