package com.tsingj.sloth.store.datalog;

import com.tsingj.sloth.common.SystemClock;
import com.tsingj.sloth.common.result.Result;
import com.tsingj.sloth.common.result.Results;
import com.tsingj.sloth.store.DataRecovery;
import com.tsingj.sloth.store.constants.LogConstants;
import com.tsingj.sloth.store.datalog.lock.LogLock;
import com.tsingj.sloth.store.datalog.lock.LogReentrantLock;
import com.tsingj.sloth.store.utils.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author yanghao
 */
public class DataLogSegment implements DataRecovery {

    private static final Logger logger = LoggerFactory.getLogger(DataLogSegment.class);

    /**
     * #文件开始offset
     * 初始化时按照文件名补齐
     */
    private long fileFromOffset;


    /**
     * #日志最大偏移量
     * load -> 根据最后一个index查找补齐
     */
    private AtomicLong largestOffset;

    /**
     * #日志最大时间戳
     * load -> 根据首个offset storeTimestamp补齐
     */
    private AtomicLong largestTimestamp;

    /**
     * #写入position
     * load -> 根据文件大小补齐
     */
    private AtomicLong wrotePosition;


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
     * fileLock
     */
    private LogLock readWriteLock;


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
    private AtomicInteger bytesSinceLastIndexAppend;

    /**
     * 索引项刷追加bytes间隔
     */
    private int logIndexIntervalBytes;


    public DataLogSegment(String logPath, long startOffset, int maxFileSize, int logIndexIntervalBytes) throws FileNotFoundException {
        this.initialization(logPath, startOffset, maxFileSize, logIndexIntervalBytes);
    }

    private void initialization(String logPath, long startOffset, int maxFileSize, int logIndexIntervalBytes) throws FileNotFoundException {
        //init log file operator
        logFile = new File(logPath + LogConstants.FileSuffix.LOG);
        this.logFileChannel = new RandomAccessFile(logFile, "rw").getChannel();
        this.readWriteLock = new LogReentrantLock();
        //init offsetIndex
        this.offsetIndex = new OffsetIndex(logPath);
        //init timeIndex
        this.timeIndex = new TimeIndex(logPath);
        logger.info("init logFile success... \n logFile:{} \n offsetIndexFile:{} \n timeIndexFile:{}.", logPath + LogConstants.FileSuffix.LOG, logPath + LogConstants.FileSuffix.OFFSET_INDEX, logPath + LogConstants.FileSuffix.TIMESTAMP_INDEX);

        this.maxFileSize = maxFileSize;
        this.largestOffset = new AtomicLong(startOffset);
        this.largestTimestamp = new AtomicLong(SystemClock.now());
        this.fileFromOffset = startOffset;
        this.wrotePosition = new AtomicLong(0);
        this.logIndexIntervalBytes = logIndexIntervalBytes;
        this.bytesSinceLastIndexAppend = new AtomicInteger(0);
    }

    //----------------------------------------------------------loadLogs--------------------------------------------------------------------

    @Override
    public void load() {
        /*
         * 根据文件信息，补齐属性
         * -- logSegment -> largestOffset  通过offsetIndex取出最后一个索引并按照索引查找至文件结尾
         * -- logSegment -> wrotePosition  当前文件大小
         * -- offsetIndex -> indexEntries  index数量
         * -- timeIndex -> indexEntries    index数量
         */
        this.offsetIndex.load();
        this.timeIndex.load();
        this.wrotePosition = new AtomicLong(this.logFile.length());
        this.loadLargestMessagePropertiesFromFile();
    }

    private void loadLargestMessagePropertiesFromFile() {
        String logPath = this.logFile.getAbsolutePath();
        Result<IndexEntry.OffsetPosition> indexFileLastOffsetResult = this.offsetIndex.getOffsetIndexFileLastOffset();
        Assert.isTrue(indexFileLastOffsetResult.success(), "load logs from offsetIndexFile " + logPath + " fail!" + indexFileLastOffsetResult.getMsg());
        IndexEntry.OffsetPosition offsetPosition = indexFileLastOffsetResult.getData();
        //2分查找获取最大offset
        long position = offsetPosition.getPosition();
        long endPosition = this.wrotePosition.get();
        long lastOffset = 0;
        long largestTimestamp = 0;
        while (position < endPosition) {
            try {
                this.readWriteLock.lock();
                //查询消息体
                logFileChannel.position(position);
                ByteBuffer headerByteBuffer = ByteBuffer.allocate(LogConstants.MessageKeyBytes.LOG_OVERHEAD);
                logFileChannel.read(headerByteBuffer);
                headerByteBuffer.rewind();
                lastOffset = headerByteBuffer.getLong();
                int storeSize = headerByteBuffer.getInt();
                ByteBuffer timestampByteBuffer = ByteBuffer.allocate(LogConstants.MessageKeyBytes.STORE_TIMESTAMP);
                logFileChannel.read(timestampByteBuffer);
                timestampByteBuffer.rewind();
                largestTimestamp = timestampByteBuffer.getLong();
                position = position + (LogConstants.MessageKeyBytes.LOG_OVERHEAD + storeSize);
            } catch (IOException e) {
                logger.error("find lastOffset IO operation fail!", e);
                throw new DataLogRecoveryException("find last offset from latest segmentFile fail!", e);
            } finally {
                this.readWriteLock.unlock();
            }
        }
        logger.info("logSegment load largestOffset:{} largestTimestamp:{} from {}.", lastOffset, largestTimestamp, logPath);
        this.largestOffset = new AtomicLong(lastOffset);
        this.largestTimestamp = new AtomicLong(largestTimestamp);
    }

    /**
     * 文件是否已经超过配置最大文件大小
     */
    public boolean isFull() {
        return wrotePosition.get() >= maxFileSize;
    }

    /**
     * 追加日志
     */
    public Result doAppend(ByteBuffer log, long offset, long storeTimestamp) {

        try {

            /*
             * 1、append log
             */
            try {
                this.readWriteLock.lock();
                this.logFileChannel.position(this.wrotePosition.get());
                this.logFileChannel.write(log);
            } finally {
                this.readWriteLock.unlock();
            }

            //记录最大偏移量插入时间
            if (largestTimestamp.get() < storeTimestamp) {
                largestOffset.set(storeTimestamp);
            }

            //记录上一次index插入后，新存入的消息
            int dataLen = log.capacity();
            long recordBytes = this.recordBytesSinceLastIndexAppend(dataLen);
            //bytesSinceLastIndexAppend为空（首次) 或者 记录字节数大于配置logIndexIntervalBytes.
            if (bytesSinceLastIndexAppend.get() == 0 || recordBytes >= this.logIndexIntervalBytes) {
                /*
                 * 2、append offset index
                 */
                this.offsetIndex.addIndex(offset, offset == this.fileFromOffset ? 0L : this.wrotePosition.get());
                /*
                 * 3、append time index
                 */
                this.timeIndex.addIndex(storeTimestamp, offset);

                //reset index append bytes.
                this.resetBytesSinceLastIndexAppend();
            }

            this.incrementWrotePosition(dataLen);

            return Results.success();
        } catch (IOException e) {
            logger.error("log append fail!", e);
            return Results.failure("log append fail!" + e.getMessage());
        }
    }

    public Result<ByteBuffer> getMessage(long offset) {
        //1、lookup logPosition slot range.
        Result<AbstractIndex.LogPositionSlotRange> lookUpResult = this.offsetIndex.lookUp(offset);
        if (lookUpResult.failure()) {
            return Results.failure(lookUpResult.getMsg());
        }
        AbstractIndex.LogPositionSlotRange logPositionRange = lookUpResult.getData();
        //2、slot logFile position find real position
        Long startPosition = logPositionRange.getStart();
        Long endPosition = logPositionRange.getEnd() != null ? logPositionRange.getEnd() : this.wrotePosition.get();
        Result<Long> logPositionResult = this.findLogPositionSlotRange(offset, startPosition, endPosition);
        if (logPositionResult.failure()) {
            return Results.failure(logPositionResult.getMsg());
        }
        //3、get log bytes
        Long position = logPositionResult.getData();
        Result<ByteBuffer> messageByPosition = this.getMessageByPosition(position);
        return messageByPosition;
    }

    public long incrementOffsetAndGet() {
        return this.largestOffset.incrementAndGet();
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
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

    public long getLargestOffset() {
        return this.largestOffset.get();
    }

    public long getLargestTimestamp() {
        return this.largestTimestamp.get();
    }

    //----------------------------------------------------------private方法--------------------------------------------------------------------

    private void incrementWrotePosition(int dataLen) {
        this.wrotePosition.addAndGet(dataLen);
    }

    private long recordBytesSinceLastIndexAppend(int dataLen) {
        return bytesSinceLastIndexAppend.addAndGet(dataLen);
    }

    private void resetBytesSinceLastIndexAppend() {
        this.bytesSinceLastIndexAppend.set(0);
    }

    private Result<ByteBuffer> getMessageByPosition(Long position) {
        try {
            this.readWriteLock.lock();
            logFileChannel.position(position);
            ByteBuffer headerByteBuffer = ByteBuffer.allocate(LogConstants.MessageKeyBytes.LOG_OVERHEAD);
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
        } finally {
            this.readWriteLock.unlock();
        }
    }

    private Result<Long> findLogPositionSlotRange(long searchOffset, Long startPosition, Long endPosition) {
        Long logPosition = null;
        long position = startPosition;
        while (position < endPosition) {
            //查询消息体
            try {
                this.readWriteLock.lock();
                logFileChannel.position(position);
                ByteBuffer headerByteBuffer = ByteBuffer.allocate(LogConstants.MessageKeyBytes.LOG_OVERHEAD);
                logFileChannel.read(headerByteBuffer);
                headerByteBuffer.rewind();
                long offset = headerByteBuffer.getLong();
                int storeSize = headerByteBuffer.getInt();
                if (searchOffset == offset) {
                    logPosition = position;
                    break;
                } else {
                    position = position + (LogConstants.MessageKeyBytes.LOG_OVERHEAD + storeSize);
                }
            } catch (IOException e) {
                logger.error("find offset:{} IO operation fail!", searchOffset, e);
                return Results.failure("find offset:" + searchOffset + " IO operation fail!");
            } finally {
                this.readWriteLock.unlock();
            }
        }

        if (logPosition == null) {
            logger.warn("file:{} , position start:{} end:{}, find offset:{} fail!", logFile.getName(), startPosition, endPosition, searchOffset);
            return Results.failure("offset " + searchOffset + " find fail!");
        }
        return Results.success(logPosition);
    }

    public void delete() {
        CommonUtil.deleteExpireFile(this.logFileChannel, this.logFile);
        this.offsetIndex.delete();
        this.timeIndex.delete();
    }

}
