package com.tsingj.sloth.store.log;

import com.tsingj.sloth.store.DataLogConstants;
import com.tsingj.sloth.store.Result;
import com.tsingj.sloth.store.Results;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author yanghao
 */
public class OffsetIndex {

    private static final Logger logger = LoggerFactory.getLogger(OffsetIndex.class);
    /**
     * offsetIndex物理文件
     */
    private File file;

    /**
     * 文件读写channel
     */
    private FileChannel fileChannel;

    /**
     * offset index 数量
     */
    private long indexEntries;

    public OffsetIndex(String logPath) throws FileNotFoundException {
        //init offsetIndex file operator
        file = new File(logPath + DataLogConstants.FileSuffix.OFFSET_INDEX);
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();

        this.indexEntries = 0L;
    }

    public void addIndex(long key, long value) throws IOException {
        /*
         * add offset index
         */
        ByteBuffer indexByteBuffer = ByteBuffer.allocate(DataLogConstants.INDEX_BYTES);
        indexByteBuffer.putLong(key);
        indexByteBuffer.putLong(value);
        this.fileChannel.position(this.getWrotePosition());
        this.fileChannel.write(indexByteBuffer);
        this.incrementIndexEntries();
    }

    private long getWrotePosition() {
        return this.indexEntries * DataLogConstants.INDEX_BYTES;
    }

    private void incrementIndexEntries() {
        this.indexEntries = this.indexEntries + 1;
    }

    //lookup logPosition slot range
    public Result<IndexEntry.OffsetPosition> lookUp(long searchKey) {
        StopWatch sw = new StopWatch();
        sw.start("getEntrySize");
        long entries = this.indexEntries;
        sw.stop();
        //index为空，返回-1
        if (entries == 0L) {
            return Results.success(new IndexEntry.OffsetPosition(0, 0));
        }
        sw.start("getIndexFileFirstOffset");
        //最小值大与查询值，从头找。  PS:务必将第一条索引插入。
        Result<IndexEntry.OffsetPosition> getFirstOffsetResult = getIndexFileFirstOffset();
        sw.stop();
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
        int i = 0;
        while (lower < upper) {
            i++;
            //这样的操作是为了让 mid 标志 取高位，否则会出现死循环
            long mid = (lower + upper + 1) / 2;
            sw.start("loop" + i);
            Result<IndexEntry.OffsetPosition> result = getIndexEntryPositionOffset(mid * DataLogConstants.INDEX_BYTES);
            sw.stop();
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

        Result<IndexEntry.OffsetPosition> lowerOffsetPositionResult = getIndexEntryPositionOffset(lower * DataLogConstants.INDEX_BYTES);
        if (lowerOffsetPositionResult.failure()) {
            return Results.failure("offset:{} can't find offsetIndex!");
        }
        offsetPosition = lowerOffsetPositionResult.getData();
        logger.debug("offset:{} find offsetIndex:{} {}", searchKey, offsetPosition.getIndexKey(), offsetPosition.getIndexValue());
        logger.debug("lookUp" + sw.prettyPrint() + "\ntotal:" + sw.getTotalTimeMillis());
        //其实这里无论返回lower 还是upper都行，循环的退出时间是lower==upper。
        return Results.success(offsetPosition);
    }

    private Result<IndexEntry.OffsetPosition> getIndexEntryPositionOffset(long indexPosition) {
        try {
            this.fileChannel.position(indexPosition);
            ByteBuffer byteBuffer = ByteBuffer.allocate(DataLogConstants.INDEX_BYTES);
            this.fileChannel.read(byteBuffer);
            byteBuffer.rewind();
            return Results.success(new IndexEntry.OffsetPosition(byteBuffer.getLong(), byteBuffer.getLong()));
        } catch (IOException e) {
            logger.error("offset indexFileReader position operation fail!", e);
            return Results.failure("offset indexFileReader position operation fail!");
        }

    }

    private Result<IndexEntry.OffsetPosition> getIndexFileFirstOffset() {
        return getIndexEntryPositionOffset(0);
    }

    protected Result<IndexEntry.OffsetPosition> getIndexFileLastOffset() {
        long entries = this.indexEntries;
        if (entries == 0L) {
            return Results.success(new IndexEntry.OffsetPosition(0, 0));
        }
        return getIndexEntryPositionOffset((entries - 1) * DataLogConstants.INDEX_BYTES);
    }

    public void loadLogs() {
        //1、load indexEntries
        this.indexEntries = this.file.length() / DataLogConstants.INDEX_BYTES;
    }

    public void flush() {
        try {
            this.fileChannel.force(true);
        } catch (IOException ignored) {
        }
    }
}
