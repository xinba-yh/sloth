package com.tsingj.sloth.store.datalog;

import com.github.benmanes.caffeine.cache.*;
import com.tsingj.sloth.store.DataRecovery;
import com.tsingj.sloth.store.constants.LogConstants;
import com.tsingj.sloth.store.datalog.lock.LogReentrantLock;
import com.tsingj.sloth.store.pojo.Result;
import com.tsingj.sloth.store.pojo.Results;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Set;

/**
 * @author yanghao
 */
public class OffsetIndex implements DataRecovery {

    private static final Logger logger = LoggerFactory.getLogger(OffsetIndex.class);

    public static final int INDEX_BYTES = 16;
    
    /**
     * offsetIndex物理文件
     */
    private final File file;

    /**
     * 文件读写channel
     */
    private final FileChannel fileChannel;

    /**
     * fileLock
     */
    private final LogReentrantLock readWriteLock;

    /**
     * caffeine index cache
     */
    private final Cache<Long, IndexEntry.OffsetPosition> warmIndexEntries;

    /**
     * 期望最大1M index缓存
     */
    private final int maxWarmIndexEntries = 1024 * 1024;

    /**
     * offset index 数量  写消息有锁，每个topic、partition同时只能插入一个消息，只保持可见即可。
     */
    private volatile long indexSize;

    public OffsetIndex(String logPath) throws FileNotFoundException {
        //init offsetIndex file operator
        this.file = new File(logPath + LogConstants.FileSuffix.OFFSET_INDEX);
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        this.readWriteLock = new LogReentrantLock();
        //init other properties
        this.indexSize = 0L;
        //1m内存 index
        this.warmIndexEntries = Caffeine.newBuilder().initialCapacity(maxWarmIndexEntries / OffsetIndex.INDEX_BYTES).recordStats().build();
    }

    @Override
    public void load() {
        long fileLength = this.file.length();
        //1、load indexEntries
        this.indexSize = fileLength / OffsetIndex.INDEX_BYTES;
        //2、load warmEntries
        long loadWarmPosition = fileLength > this.maxWarmIndexEntries ? fileLength - this.maxWarmIndexEntries : 0L;
        logger.info("prepare load offsetIndex from position:{}", loadWarmPosition);
        while (loadWarmPosition < fileLength) {
            Result<IndexEntry.OffsetPosition> indexEntryPositionOffset = getIndexEntryByIndexPosition(loadWarmPosition, false);
            if (indexEntryPositionOffset.failure()) {
                logger.warn("load index fail! {}", indexEntryPositionOffset.getMsg());
                break;
            }
            IndexEntry.OffsetPosition data = indexEntryPositionOffset.getData();
            this.warmIndexEntries.put(loadWarmPosition, data);
            loadWarmPosition = loadWarmPosition + OffsetIndex.INDEX_BYTES;
        }
        logger.info("load offsetIndex success, current indexSize:{} warm indexEntrySize:{}", indexSize, this.warmIndexEntries.estimatedSize());
    }

    public void addIndex(long key, long value) throws IOException {
        /*
         * add offset index
         */
        ByteBuffer indexByteBuffer = ByteBuffer.allocate(OffsetIndex.INDEX_BYTES);
        indexByteBuffer.putLong(key);
        indexByteBuffer.putLong(value);
        indexByteBuffer.flip();

        try {
            this.readWriteLock.lock();
            this.fileChannel.position(this.getWrotePosition());
            this.fileChannel.write(indexByteBuffer);
            this.warmIndexEntries.put(this.getWrotePosition(), new IndexEntry.OffsetPosition(key, value));
            this.incrementIndexEntries();
        } finally {
            this.readWriteLock.unlock();
        }

    }

    //lookup logPosition slot range
    public Result<LogPositionSlotRange> lookUp(long searchKey) {
        //1、get lower indexEntry
        long entries = this.indexSize;
        //index为空，返回-1
        if (entries == 0L) {
            return Results.success(new LogPositionSlotRange(0L, null));
        }
        //最小值大与查询值，从头找。  PS:务必将第一条索引插入。
        Result<IndexEntry.OffsetPosition> getFirstOffsetResult = getIndexFileFirstOffset();
        if (!getFirstOffsetResult.success()) {
            return Results.failure(getIndexFileFirstOffset().getMsg());
        }

        long startOffset = getFirstOffsetResult.getData().getIndexKey();
        if (startOffset >= searchKey) {
            return Results.success(new LogPositionSlotRange(0L, null));
        }

        //开始二分查找 <= searchOffset的最大值
        long lower = 0L;
        long upper = entries - 1;
        IndexEntry.OffsetPosition startOffsetPosition;
        while (lower < upper) {
            //这样的操作是为了让 mid 标志 取高位，否则会出现死循环
            long mid = (lower + upper + 1) / 2;
            Result<IndexEntry.OffsetPosition> result = getIndexEntryByIndexPosition(mid * OffsetIndex.INDEX_BYTES);
            if (!result.success()) {
                return Results.failure(result.getMsg());
            }
            startOffsetPosition = result.getData();
            long found = startOffsetPosition.getIndexKey();
            if (found <= searchKey) {
                lower = mid;
            } else {
                upper = mid - 1;
            }
        }

        Result<IndexEntry.OffsetPosition> lowerOffsetPositionResult = getIndexEntryByIndexPosition(lower * OffsetIndex.INDEX_BYTES);
        if (lowerOffsetPositionResult.failure()) {
            return Results.failure("offset:{} can't find offsetIndex!");
        }
        //2、get next indexEntry if exist.
        IndexEntry.OffsetPosition endOffsetPosition = null;
        long realUpper = entries - 1;
        if (lower < realUpper) {
            Result<IndexEntry.OffsetPosition> nextOffsetPositionResult = getIndexEntryByIndexPosition((lower + 1) * OffsetIndex.INDEX_BYTES);
            if (nextOffsetPositionResult.failure()) {
                return Results.failure("offset:{} can't find offsetIndex!");
            }
            endOffsetPosition = nextOffsetPositionResult.getData();
        }//其他条件仅为：lower = upper 没有下一个offsetIndex了。

        startOffsetPosition = lowerOffsetPositionResult.getData();
        logger.debug("offset:{} find startOffsetIndex:{} {} endOffsetIndex:{} {}", searchKey, startOffsetPosition.getOffset(), startOffsetPosition.getPosition(), endOffsetPosition != null ? endOffsetPosition.getOffset() : "null", endOffsetPosition != null ? endOffsetPosition.getPosition() : "null");
        //其实这里无论返回lower 还是upper都行，循环的退出时间是lower==upper。
        return Results.success(new LogPositionSlotRange(startOffsetPosition.getPosition(), endOffsetPosition != null ? endOffsetPosition.getPosition() : null));
    }

    private Result<IndexEntry.OffsetPosition> getIndexEntryByIndexPosition(long indexPosition) {
        return getIndexEntryByIndexPosition(indexPosition, true);
    }

    private Result<IndexEntry.OffsetPosition> getIndexEntryByIndexPosition(long indexPosition, boolean useCache) {
        if (useCache) {
            IndexEntry.OffsetPosition offsetPosition = this.warmIndexEntries.getIfPresent(indexPosition);
            if (offsetPosition != null) {
                return Results.success(offsetPosition);
            }
        }
        try {
            this.readWriteLock.lock();
            this.fileChannel.position(indexPosition);
            ByteBuffer byteBuffer = ByteBuffer.allocate(OffsetIndex.INDEX_BYTES);
            this.fileChannel.read(byteBuffer);
            byteBuffer.rewind();
            return Results.success(new IndexEntry.OffsetPosition(byteBuffer.getLong(), byteBuffer.getLong()));
        } catch (IOException e) {
            logger.error("offset indexFileReader position operation fail!", e);
            return Results.failure("offset indexFileReader position operation fail!");
        } finally {
            this.readWriteLock.unlock();
        }
    }

    private Result<IndexEntry.OffsetPosition> getIndexFileFirstOffset() {
        return getIndexEntryByIndexPosition(0);
    }

    protected Result<IndexEntry.OffsetPosition> getIndexFileLastOffset() {
        long entries = this.indexSize;
        if (entries == 0L) {
            return Results.success(new IndexEntry.OffsetPosition(0, 0));
        }
        return getIndexEntryByIndexPosition((entries - 1) * OffsetIndex.INDEX_BYTES, false);
    }

    public void flush() {
        try {
            this.readWriteLock.lock();
            this.fileChannel.force(true);
        } catch (IOException ignored) {
        } finally {
            this.readWriteLock.unlock();
        }
    }

    public void freeNoWarmIndexCache() {
        //计算超出maxWarmIndex长度
        long cleanupCount = this.warmIndexEntries.estimatedSize() - this.maxWarmIndexEntries / OffsetIndex.INDEX_BYTES;
        if (cleanupCount > 0) {
            //this set is sorted
            Set<@NonNull Long> positions = warmIndexEntries.asMap().keySet();
            int i = 0;
            for (Long key : positions) {
                if (i >= cleanupCount) {
                    break;
                }
                this.warmIndexEntries.invalidate(key);
                i++;
            }
            logger.info("cleanup no warm index cache.");
        }
    }

    public void showIndexCacheStats() {
        logger.info("hitCount:" + this.warmIndexEntries.stats().hitCount() + " missCount:" + this.warmIndexEntries.stats().missCount());
    }


    private long getWrotePosition() {
        return this.indexSize * OffsetIndex.INDEX_BYTES;
    }

    private void incrementIndexEntries() {
        this.indexSize = this.indexSize + 1;
    }


    public static class LogPositionSlotRange {

        public LogPositionSlotRange(Long start, Long end) {
            this.start = start;
            this.end = end;
        }

        private Long start;

        private Long end;

        public Long getStart() {
            return start;
        }

        public Long getEnd() {
            return end;
        }

    }

}
