package com.tsingj.sloth.store.log;

import com.tsingj.sloth.store.*;
import com.tsingj.sloth.store.utils.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.StopWatch;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author yanghao
 */
public class LogSegment {

    private static final Logger logger = LoggerFactory.getLogger(LogSegment.class);

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

    private FileChannel logFileChannel;


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
        this.initialization(logPath, startOffset, maxFileSize, logIndexIntervalBytes);
    }

    public static LogSegment loadLogs(File segmentFile, int maxFileSize, int logIndexIntervalBytes) throws FileNotFoundException {
        String logPath = segmentFile.getAbsolutePath().replace(DataLogConstants.FileSuffix.LOG, "");
        long startOffset = CommonUtil.fileName2Offset(segmentFile.getName());
        /*
         * 1、初始化LogSegment、OffsetIndex、TimeIndex
         */
        LogSegment logSegment = new LogSegment(logPath, startOffset, maxFileSize, logIndexIntervalBytes);
        /*
         * 2、根据文件信息，补齐属性
         * -- logSegment -> currentOffset  通过offsetIndex取出最后一个索引并按照索引查找至文件结尾
         * -- logSegment -> wrotePosition  当前文件大小
         * -- offsetIndex -> indexEntries  index数量
         * -- timeIndex -> indexEntries    index数量
         */
        logSegment.offsetIndex.loadLogs();
        logSegment.timeIndex.loadLogs();
        logSegment.loadCurrentOffsetFromFile(logPath);
        logSegment.wrotePosition = segmentFile.length();
        return logSegment;
    }

    private void loadCurrentOffsetFromFile(String logPath) {
        Result<IndexEntry.OffsetPosition> indexFileLastOffsetResult = this.offsetIndex.getIndexFileLastOffset();
        Assert.isTrue(indexFileLastOffsetResult.success(), "load logs from offsetIndexFile " + logPath + " fail!" + indexFileLastOffsetResult.getMsg());
        IndexEntry.OffsetPosition offsetPosition = indexFileLastOffsetResult.getData();
        long position = offsetPosition.getPosition();
        Assert.isTrue(indexFileLastOffsetResult.success(), "load logs from offsetIndexFile " + logPath + " fail!" + indexFileLastOffsetResult.getMsg());
        Long offset = this.findLastOffset(position);
        logger.info("logSegment load currentOffset:{} from {}.", offset, logPath);
        this.currentOffset = offset;
    }

    private long findLastOffset(long startPosition) {
        long position = startPosition;
        long endPosition = this.wrotePosition;
        long lastOffset = 0;
        try {
            while (position < endPosition) {
                //查询消息体
                logFileChannel.position(position);
                ByteBuffer headerByteBuffer = ByteBuffer.allocate(DataLogConstants.MessageKeyBytes.LOG_OVERHEAD);
                logFileChannel.read(headerByteBuffer);
                headerByteBuffer.rewind();
                lastOffset = headerByteBuffer.getLong();
                int storeSize = headerByteBuffer.getInt();
                position = position + (DataLogConstants.MessageKeyBytes.LOG_OVERHEAD + storeSize);
            }
            return lastOffset;
        } catch (IOException e) {
            logger.error("find lastOffset IO operation fail!", e);
            //todo 新定义loadLogException
            throw new RuntimeException(e);
        }
    }

    private void initialization(String logPath, long startOffset, int maxFileSize, int logIndexIntervalBytes) throws FileNotFoundException {
        //init log file operator
        logFile = new File(logPath + DataLogConstants.FileSuffix.LOG);
        this.logFileChannel = new RandomAccessFile(logFile, "rw").getChannel();
        //init offsetIndex
        this.offsetIndex = new OffsetIndex(logPath);
        //init timeIndex
        this.timeIndex = new TimeIndex(logPath);
        logger.info("init logFile success... \n logFile:{} \n offsetIndexFile:{} \n timeIndexFile:{}.", logPath + DataLogConstants.FileSuffix.LOG, logPath + DataLogConstants.FileSuffix.OFFSET_INDEX, logPath + DataLogConstants.FileSuffix.TIMESTAMP_INDEX);

        this.maxFileSize = maxFileSize;
        this.currentOffset = startOffset;
        this.fileFromOffset = startOffset;
        this.wrotePosition = 0;
        this.logIndexIntervalBytes = logIndexIntervalBytes;
    }

    public boolean isFull() {
        return wrotePosition >= maxFileSize;
    }

    public Result doAppend(ByteBuffer log, long offset, long storeTimestamp) {
        try {
            /*
             * 1、append log
             */
            this.logFileChannel.position(this.wrotePosition);
            this.logFileChannel.write(log);

            //记录上一次index插入后，新存入的消息
            int dataLen = log.capacity();
            long recordBytes = this.recordBytesSinceLastIndexAppend(dataLen);
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
            incrementWrotePosition(dataLen);

            return Results.success();
        } catch (IOException e) {
            logger.error("log append fail!", e);
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
        StopWatch sw = new StopWatch();
        sw.start("offsetIndex.lookUp");
        //1、lookup logPosition slot range
        Result<IndexEntry.OffsetPosition> lookUpResult = this.offsetIndex.lookUp(offset);
        if (lookUpResult.failure()) {
            return Results.failure(lookUpResult.getMsg());
        }
        sw.stop();
        //2、slot logFile position find real position
        // TODO: 2022/2/25 add endPosition. maxMessageSize+slotSize || lookUp返回下一个索引的下标
        sw.start("findLogPositionSlotRange");
        IndexEntry.OffsetPosition indexEntry = lookUpResult.getData();
        Long startPosition = indexEntry.getPosition();
        Long endPosition = this.wrotePosition;
        Result<Long> logPositionResult = this.findLogPositionSlotRange(offset, startPosition, endPosition);
        //3、get log bytes
        if (logPositionResult.failure()) {
            return Results.failure(logPositionResult.getMsg());
        }
        Long position = logPositionResult.getData();
        sw.stop();
//        sw.start("getMessageByPosition");
        Result<ByteBuffer> messageByPosition = this.getMessageByPosition(position);
//        sw.stop();
        logger.debug("getMessage:" + sw.prettyPrint() + "\n total:" + sw.getTotalTimeMillis());
        return messageByPosition;
    }

    private Result<ByteBuffer> getMessageByPosition(Long position) {
        try {
            logFileChannel.position(position);
            ByteBuffer headerByteBuffer = ByteBuffer.allocate(DataLogConstants.MessageKeyBytes.LOG_OVERHEAD);
            logFileChannel.read(headerByteBuffer);
            headerByteBuffer.flip();

            long offset = headerByteBuffer.getLong();
            int storeSize = headerByteBuffer.getInt();
            ByteBuffer storeByteBuffer = ByteBuffer.allocate(storeSize);
            logFileChannel.read(storeByteBuffer);
            //position reset
            storeByteBuffer.flip();
            return Results.success(storeByteBuffer);
        } catch (IOException e) {
            logger.error("get message by position {} , IO operation fail!", position, e);
            return Results.failure("get message by position {} IO operation fail!");
        }
    }

    private Result<Long> findLogPositionSlotRange(long searchOffset, Long startPosition, Long endPosition) {
        Long logPosition = null;
        long position = startPosition;
        while (position < endPosition) {
            //查询消息体
            try {
                logFileChannel.position(position);
                ByteBuffer headerByteBuffer = ByteBuffer.allocate(DataLogConstants.MessageKeyBytes.LOG_OVERHEAD);
                logFileChannel.read(headerByteBuffer);
                headerByteBuffer.rewind();
                long offset = headerByteBuffer.getLong();
                int storeSize = headerByteBuffer.getInt();
                if (searchOffset == offset) {
                    logPosition = position;
                    break;
                } else {
                    position = position + (DataLogConstants.MessageKeyBytes.LOG_OVERHEAD + storeSize);
                }
            } catch (IOException e) {
                logger.error("find offset:{} IO operation fail!", searchOffset, e);
                return Results.failure("find offset:" + searchOffset + " IO operation fail!");
            }
        }

        if (logPosition == null) {
            logger.warn("file:{} , position start:{} end:{}, find offset:{} fail!", logFile.getName(), startPosition, endPosition, searchOffset);
            return Results.failure("offset " + searchOffset + " find fail!");
        }
        return Results.success(logPosition);
    }


    public OffsetIndex getOffsetIndex() {
        return this.offsetIndex;
    }

    public TimeIndex getTimeIndex() {
        return this.timeIndex;
    }

    public void flush() {
        try {
            this.logFileChannel.force(true);
        } catch (IOException ignored) {
        }
    }


}
