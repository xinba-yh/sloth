package com.tsingj.sloth.store.datalog;

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

    private FileChannel logReader;

    private BufferedOutputStream logWriter;

    /**
     * offsetIndex物理文件
     */
    private File offsetIndexFile;

    private FileChannel offsetIndexReader;

    private BufferedOutputStream offsetIndexWriter;

    /**
     * timeIndex物理文件
     */
    private File timeIndexFile;

    private FileChannel timeIndexReader;

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
        //init log file operator
        logFile = new File(logPath);
        this.logWriter = new BufferedOutputStream(new FileOutputStream(logFile, true));
        this.logReader = new RandomAccessFile(logFile, "r").getChannel();
        //init offsetIndex file operator
        offsetIndexFile = new File(offsetIndexPath);
        this.offsetIndexWriter = new BufferedOutputStream(new FileOutputStream(offsetIndexFile, true));
        this.offsetIndexReader = new RandomAccessFile(offsetIndexFile, "r").getChannel();
        //init timeIndex file operator
        timeIndexFile = new File(timeIndexPath);
        this.timeIndexWriter = new BufferedOutputStream(new FileOutputStream(timeIndexFile, true));
        this.timeIndexReader = new RandomAccessFile(timeIndexFile, "r").getChannel();
        log.info("init logFile success... \n logFile:{} \n offsetIndexFile:{} \n timeIndexFile:{}.", logPath, offsetIndexPath, timeIndexPath);

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

            // TODO: 2022/2/25 抽象至offsetIndex和timestampIndex
            //记录上一次index插入后，新存入的消息
            long recordBytes = this.recordBytesSinceLastIndexAppend(data.length);
            if (offset == 0 || recordBytes >= this.logIndexIntervalBytes) {
                /*
                 * 2、append offset index
                 */
                ByteBuffer indexByteBuffer = ByteBuffer.allocate(DataLogConstants.INDEX_BYTES);
                indexByteBuffer.putLong(offset);
                indexByteBuffer.putLong(offset == 0 ? 0 : this.wrotePosition + 1);
                offsetIndexWriter.write(indexByteBuffer.array());
                offsetIndexWriter.flush();
                /*
                 * 3、append time index
                 */
                ByteBuffer timeIndexByteBuffer = ByteBuffer.allocate(DataLogConstants.INDEX_BYTES);
                timeIndexByteBuffer.putLong(storeTimestamp);
                timeIndexByteBuffer.putLong(offset);
                this.timeIndexWriter.write(timeIndexByteBuffer.array());
                offsetIndexWriter.flush();

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
        Result<Long> lookUpResult = this.indexLookUpStartPosition(offset, (int) (FileUtils.sizeOf(offsetIndexFile) / DataLogConstants.INDEX_BYTES));
        if (lookUpResult.failure()) {
            return Results.failure(lookUpResult.getMsg());
        }
        //2、slot logFile position find real position
        // TODO: 2022/2/25 add endPosition. maxMessageSize+slotSize || lookUp返回下一个索引的下标
        Long startPosition = lookUpResult.getData();
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

    //lookup logPosition slot range
    private Result<Long> indexLookUpStartPosition(long searchOffset, int entries) {
        //index为空，返回-1
        if (entries == 0) {
            return Results.success(0L);
        }
        //最小值大与查询值，从头找。  PS:务必将第一条索引插入。
        Result<Long> getFirstOffsetResult = getIndexFileFirstOffset();
        if (!getFirstOffsetResult.success()) {
            return getFirstOffsetResult;
        }
        long startOffset = getFirstOffsetResult.getData();
        if (startOffset >= searchOffset) {
            return Results.success(0L);
        }
        //开始二分查找 <= searchOffset的最大值
        long lower = 0L;
        long upper = entries - 1;
        while (lower < upper) {
            //这样的操作是为了让 mid 标志 取高位，否则会出现死循环
            long mid = (lower + upper + 1) / 2;
            Result<Long> result = getIndexFilePositionOffset(mid * DataLogConstants.INDEX_BYTES);
            if (!result.success()) {
                return result;
            }
            long found = result.getData();
            if (found <= searchOffset) {
                lower = mid;
            } else {
                upper = mid - 1;
            }
        }
        //其实这里无论返回lower 还是upper都行，循环的退出时间是lower==upper。
        return Results.success(lower * DataLogConstants.INDEX_BYTES);
    }

    private Result<Long> getIndexFilePositionOffset(long indexPosition) {
        try {
            this.offsetIndexReader.position(indexPosition + DataLogConstants.IndexKeyBytes.KEY);
            ByteBuffer byteBuffer = ByteBuffer.allocate(DataLogConstants.IndexKeyBytes.VALUE);
            offsetIndexReader.read(byteBuffer);
            byteBuffer.rewind();
            return Results.success(byteBuffer.getLong());
        } catch (IOException e) {
            log.error("offset indexFileReader position operation fail!", e);
            return Results.failure("offset indexFileReader position operation fail!");
        }

    }

    private Result<Long> getIndexFileFirstOffset() {
        return getIndexFilePositionOffset(0);
    }

}
