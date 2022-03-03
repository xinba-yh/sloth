package com.tsingj.sloth.store.log;

import com.github.benmanes.caffeine.cache.*;
import com.tsingj.sloth.store.constants.LogConstants;
import com.tsingj.sloth.store.log.lock.LogReentrantLock;
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
public class OffsetIndex {

    private static final Logger logger = LoggerFactory.getLogger(OffsetIndex.class);
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
     * offset index 数量
     */
    private long indexSize;

    public OffsetIndex(String logPath) throws FileNotFoundException {
        //init offsetIndex file operator
        this.file = new File(logPath + LogConstants.FileSuffix.OFFSET_INDEX);
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        this.readWriteLock = new LogReentrantLock();
        //init other properties
        this.indexSize = 0L;
        //1m内存 index
        this.warmIndexEntries = Caffeine.newBuilder().initialCapacity(maxWarmIndexEntries / LogConstants.INDEX_BYTES).recordStats().build();

    }

    public void addIndex(long key, long value) throws IOException {
        /*
         * add offset index
         */
        ByteBuffer indexByteBuffer = ByteBuffer.allocate(LogConstants.INDEX_BYTES);
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

    private long getWrotePosition() {
        return this.indexSize * LogConstants.INDEX_BYTES;
    }

    private void incrementIndexEntries() {
        this.indexSize = this.indexSize + 1;
    }

    //lookup logPosition slot range
    public Result<IndexEntry.OffsetPosition> lookUp(long searchKey) {
        long entries = this.indexSize;
        //index为空，返回-1
        if (entries == 0L) {
            return Results.success(new IndexEntry.OffsetPosition(0, 0));
        }
        //最小值大与查询值，从头找。  PS:务必将第一条索引插入。
        Result<IndexEntry.OffsetPosition> getFirstOffsetResult = getIndexFileFirstOffset();
        if (!getFirstOffsetResult.success()) {
            return Results.failure(getIndexFileFirstOffset().getMsg());
        }

        long startOffset = getFirstOffsetResult.getData().getIndexKey();
        if (startOffset >= searchKey) {
            return Results.success(new IndexEntry.OffsetPosition(0, 0));
        }

        //开始二分查找 <= searchOffset的最大值
        long lower = 0L;
        long upper = entries - 1;
        IndexEntry.OffsetPosition offsetPosition;
        while (lower < upper) {
            //这样的操作是为了让 mid 标志 取高位，否则会出现死循环
            long mid = (lower + upper + 1) / 2;
            Result<IndexEntry.OffsetPosition> result = getIndexEntryPositionOffset(mid * LogConstants.INDEX_BYTES);
            if (!result.success()) {
                return Results.failure(result.getMsg());
            }
            offsetPosition = result.getData();
            long found = offsetPosition.getIndexKey();
            if (found <= searchKey) {
                lower = mid;
            } else {
                upper = mid - 1;
            }
        }

        Result<IndexEntry.OffsetPosition> lowerOffsetPositionResult = getIndexEntryPositionOffset(lower * LogConstants.INDEX_BYTES);
        if (lowerOffsetPositionResult.failure()) {
            return Results.failure("offset:{} can't find offsetIndex!");
        }
        offsetPosition = lowerOffsetPositionResult.getData();
        logger.debug("offset:{} find offsetIndex:{} {}", searchKey, offsetPosition.getIndexKey(), offsetPosition.getIndexValue());
        //其实这里无论返回lower 还是upper都行，循环的退出时间是lower==upper。
        return Results.success(offsetPosition);
    }

    private Result<IndexEntry.OffsetPosition> getIndexEntryPositionOffset(long indexPosition) {
        return getIndexEntryPositionOffset(indexPosition, true);
    }

    private Result<IndexEntry.OffsetPosition> getIndexEntryPositionOffset(long indexPosition, boolean useCache) {
        if (useCache) {
            IndexEntry.OffsetPosition offsetPosition = this.warmIndexEntries.getIfPresent(indexPosition);
            if (offsetPosition != null) {
                return Results.success(offsetPosition);
            }
        }
        try {
            this.readWriteLock.lock();
            this.fileChannel.position(indexPosition);
            ByteBuffer byteBuffer = ByteBuffer.allocate(LogConstants.INDEX_BYTES);
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
        return getIndexEntryPositionOffset(0);
    }

    protected Result<IndexEntry.OffsetPosition> getIndexFileLastOffset() {
        long entries = this.indexSize;
        if (entries == 0L) {
            return Results.success(new IndexEntry.OffsetPosition(0, 0));
        }
        return getIndexEntryPositionOffset((entries - 1) * LogConstants.INDEX_BYTES, false);
    }

    public void loadLogs() {
        long fileLength = this.file.length();
        //1、load indexEntries
        this.indexSize = fileLength / LogConstants.INDEX_BYTES;
        //2、load warmEntries
        long loadWarmPosition = fileLength > this.maxWarmIndexEntries ? fileLength - this.maxWarmIndexEntries : 0L;
        logger.info("prepare load offsetIndex from position:{}", loadWarmPosition);
        while (loadWarmPosition < fileLength) {
            Result<IndexEntry.OffsetPosition> indexEntryPositionOffset = getIndexEntryPositionOffset(loadWarmPosition, false);
            if (indexEntryPositionOffset.failure()) {
                logger.warn("load index fail! {}", indexEntryPositionOffset.getMsg());
                break;
            }
            IndexEntry.OffsetPosition data = indexEntryPositionOffset.getData();
            this.warmIndexEntries.put(loadWarmPosition, data);
            loadWarmPosition = loadWarmPosition + LogConstants.INDEX_BYTES;
        }
        logger.info("load offsetIndex success, current indexSize:{} warm indexEntrySize:{}", indexSize, this.warmIndexEntries.estimatedSize());
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
        long cleanupCount = this.warmIndexEntries.estimatedSize() - this.maxWarmIndexEntries / LogConstants.INDEX_BYTES;
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
        }
    }

    public void showIndexCacheStats() {
        logger.info("hitCount:" + this.warmIndexEntries.stats().hitCount() + " missCount:" + this.warmIndexEntries.stats().missCount());
    }

}
