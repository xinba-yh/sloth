package com.tsingj.sloth.store.datalog;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.tsingj.sloth.common.result.Result;
import com.tsingj.sloth.common.result.Results;
import com.tsingj.sloth.store.DataRecovery;
import com.tsingj.sloth.store.constants.LogConstants;
import com.tsingj.sloth.store.datalog.lock.LogReentrantLock;
import com.tsingj.sloth.store.utils.CommonUtil;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author yanghao
 */
public abstract class AbstractIndex {

    private static final Logger logger = LoggerFactory.getLogger(AbstractIndex.class);

    protected static final int INDEX_BYTES = 16;

    /**
     * offsetIndex物理文件
     */
    protected final File file;

    /**
     * 文件读写channel
     */
    protected final FileChannel fileChannel;

    /**
     * fileLock
     */
    protected final LogReentrantLock readWriteLock;

    /**
     * caffeine index cache
     */
    protected final Cache<Long, IndexEntry> warmIndexEntries;

    /**
     * 期望最大1M index缓存
     */
    protected final int maxWarmIndexEntries = 1024 * 1024;

    /**
     * offset index 数量  写消息有锁，每个topic、partition同时只能插入一个消息，只保持可见即可。
     */
    protected AtomicLong indexSize;

    public AbstractIndex(String logPath) throws FileNotFoundException {
        //init offsetIndex file operator
        this.file = new File(logPath + this.indexType());
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        this.readWriteLock = new LogReentrantLock();
        //init other properties
        this.indexSize = new AtomicLong(0);
        //1m内存 index
        this.warmIndexEntries = Caffeine.newBuilder().initialCapacity(maxWarmIndexEntries / INDEX_BYTES).recordStats().build();
    }

    public void load() {
        long fileLength = this.file.length();
        long errorSize = fileLength % INDEX_BYTES;
        //如果文件大小不是INDEX_BYTES倍数，截断.
        if (errorSize != 0) {
            fileLength = fileLength - errorSize;
            try {
                this.fileChannel.truncate(fileLength);
            } catch (IOException e) {
                throw new DataLogRecoveryException("recovery index:" + this.file.getAbsolutePath() + " fail!", e);
            }
        }
        //1、load indexEntries
        this.indexSize = new AtomicLong(fileLength / INDEX_BYTES);
        //2、load warmEntries
        long loadWarmPosition = fileLength > this.maxWarmIndexEntries ? fileLength - this.maxWarmIndexEntries : 0L;
        logger.info("prepare load {} from position:{}", this.indexType(), loadWarmPosition);
        while (loadWarmPosition < fileLength) {
            Result<IndexEntry> indexEntryResult = getIndexEntryByIndexPosition(loadWarmPosition, false);
            if (indexEntryResult.failure()) {
                logger.warn("load index fail! {}", indexEntryResult.getMsg());
                break;
            }
            IndexEntry data = indexEntryResult.getData();
            this.warmIndexEntries.put(loadWarmPosition, data);
            loadWarmPosition = loadWarmPosition + INDEX_BYTES;
        }
        logger.info("load {} success, current indexSize:{} warm indexEntrySize:{}", this.indexType(), indexSize, this.warmIndexEntries.estimatedSize());
    }

    protected abstract String indexType();

    public void addIndex(long key, long value) throws IOException {
        /*
         * add offset index
         */
        ByteBuffer indexByteBuffer = ByteBuffer.allocate(INDEX_BYTES);
        indexByteBuffer.putLong(key);
        indexByteBuffer.putLong(value);
        indexByteBuffer.flip();

        try {
            this.readWriteLock.lock();
            this.fileChannel.position(this.getWrotePosition());
            this.fileChannel.write(indexByteBuffer);
            this.warmIndexEntries.put(this.getWrotePosition(), new IndexEntry(key, value));
            this.indexSize.incrementAndGet();
        } finally {
            this.readWriteLock.unlock();
        }
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


    //lookup logPosition slot range
    public Result<LogPositionSlotRange> lookUp(long searchKey) {
        //1、get lower indexEntry
        long entries = this.indexSize.get();
        //index为空，返回-1
        if (entries == 0L) {
            return Results.success(new LogPositionSlotRange(0L, null));
        }
        //最小值大与查询值，从头找。  PS:务必将第一条索引插入。
        Result<IndexEntry> getFirstOffsetResult = getIndexFileFirstOffset();
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
        IndexEntry startIndexEntry;
        while (lower < upper) {
            //这样的操作是为了让 mid 标志 取高位，否则会出现死循环
            long mid = (lower + upper + 1) / 2;
            Result<IndexEntry> result = getIndexEntryByIndexPosition(mid * INDEX_BYTES);
            if (!result.success()) {
                return Results.failure(result.getMsg());
            }
            startIndexEntry = result.getData();
            long found = startIndexEntry.getIndexKey();
            if (found <= searchKey) {
                lower = mid;
            } else {
                upper = mid - 1;
            }
        }

        Result<IndexEntry> lowerIndexEntryResult = getIndexEntryByIndexPosition(lower * INDEX_BYTES);
        if (lowerIndexEntryResult.failure()) {
            return Results.failure("can't find index!");
        }
        //2、get next indexEntry if exist.
        IndexEntry endIndexEntry = null;
        long realUpper = entries - 1;
        if (lower < realUpper) {
            Result<IndexEntry> nextIndexEntryResult = getIndexEntryByIndexPosition((lower + 1) * INDEX_BYTES);
            if (nextIndexEntryResult.failure()) {
                return Results.failure("can't find index!");
            }
            endIndexEntry = nextIndexEntryResult.getData();
        }//其他条件仅为：lower = upper 没有下一个offsetIndex了。

        startIndexEntry = lowerIndexEntryResult.getData();
        logger.debug("index:{} find startIndex:{} {} endIndex:{} {}", searchKey, startIndexEntry.getIndexKey(), startIndexEntry.getIndexValue(), endIndexEntry != null ? endIndexEntry.getIndexValue() : "null", endIndexEntry != null ? endIndexEntry.getIndexValue() : "null");
        //其实这里无论返回lower 还是upper都行，循环的退出时间是lower==upper。
        return Results.success(new LogPositionSlotRange(startIndexEntry.getIndexValue(), endIndexEntry != null ? endIndexEntry.getIndexValue() : null));
    }

    public void delete() {
        CommonUtil.deleteExpireFile(this.fileChannel, this.file);
    }

    public static class LogPositionSlotRange {

        public LogPositionSlotRange(Long start, Long end) {
            this.start = start;
            this.end = end;
        }

        private final Long start;

        private final Long end;

        public Long getStart() {
            return start;
        }

        public Long getEnd() {
            return end;
        }

    }


    protected long getWrotePosition() {
        return this.indexSize.get() * INDEX_BYTES;
    }

    public void showIndexCacheStats() {
        logger.info("hitCount:" + this.warmIndexEntries.stats().hitCount() + " missCount:" + this.warmIndexEntries.stats().missCount());
    }


    public void freeNoWarmIndexCache() {
        //计算超出maxWarmIndex长度
        long cleanupCount = this.warmIndexEntries.estimatedSize() - this.maxWarmIndexEntries / INDEX_BYTES;
        if (cleanupCount > 0) {
            //this set is sorted
            Set<@NonNull Long> warmIndexKeys = warmIndexEntries.asMap().keySet();
            int i = 0;
            for (Long key : warmIndexKeys) {
                if (i >= cleanupCount) {
                    break;
                }
                this.warmIndexEntries.invalidate(key);
                i++;
            }
            logger.info("cleanup no warm index cache.");
        }
    }

    protected Result<IndexEntry> getIndexEntryByIndexPosition(long indexPosition) {
        return getIndexEntryByIndexPosition(indexPosition, true);
    }

    protected Result<IndexEntry> getIndexEntryByIndexPosition(long indexPosition, boolean useCache) {
        if (useCache) {
            IndexEntry indexEntry = this.warmIndexEntries.getIfPresent(indexPosition);
            if (indexEntry != null) {
                return Results.success(indexEntry);
            }
        }
        try {
            this.readWriteLock.lock();
            this.fileChannel.position(indexPosition);
            ByteBuffer byteBuffer = ByteBuffer.allocate(INDEX_BYTES);
            this.fileChannel.read(byteBuffer);
            byteBuffer.rewind();
            return Results.success(new IndexEntry(byteBuffer.getLong(), byteBuffer.getLong()));
        } catch (IOException e) {
            logger.error("{} indexFileReader position operation fail!", this.indexType(), e);
            return Results.failure(this.indexType() + " indexFileReader position operation fail!");
        } finally {
            this.readWriteLock.unlock();
        }
    }

    protected Result<IndexEntry> getIndexFileFirstOffset() {
        return getIndexEntryByIndexPosition(0);
    }

    protected Result<IndexEntry> getIndexFileLastOffset() {
        long entries = this.indexSize.get();
        if (entries == 0L) {
            return Results.success(new IndexEntry(0, 0));
        }
        return getIndexEntryByIndexPosition((entries - 1) * INDEX_BYTES, false);
    }

}
