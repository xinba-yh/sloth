package com.tsingj.sloth.store.log;

import com.tsingj.sloth.store.DataLogConstants;
import com.tsingj.sloth.store.Result;
import com.tsingj.sloth.store.Results;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private FileChannel fileReader;

    private BufferedOutputStream fileWriter;

    public OffsetIndex(String logPath) throws FileNotFoundException {
        //init offsetIndex file operator
        file = new File(logPath + DataLogConstants.FileSuffix.OFFSET_INDEX);
        this.fileWriter = new BufferedOutputStream(new FileOutputStream(file, true));
        this.fileReader = new RandomAccessFile(file, "r").getChannel();
    }

    public void addIndex(long key, long value) throws IOException {
        /*
         * add offset index
         */
        ByteBuffer indexByteBuffer = ByteBuffer.allocate(DataLogConstants.INDEX_BYTES);
        indexByteBuffer.putLong(key);
        indexByteBuffer.putLong(value);
        this.fileWriter.write(indexByteBuffer.array());
        this.fileWriter.flush();
    }

    public long getFileSize() {
        return FileUtils.sizeOf(file);
    }

    private long getEntrySize() {
        return this.getFileSize() / DataLogConstants.INDEX_BYTES;
    }

    //lookup logPosition slot range
    public Result<IndexEntry.OffsetPosition> lookUp(long searchKey) {
        long entries = getEntrySize();
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
        IndexEntry.OffsetPosition offsetPosition = null;
        while (lower < upper) {
            //这样的操作是为了让 mid 标志 取高位，否则会出现死循环
            long mid = (lower + upper + 1) / 2;
            Result<IndexEntry.OffsetPosition> result = getIndexEntryPositionOffset(mid * DataLogConstants.INDEX_BYTES);
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
        if(offsetPosition == null){
            return Results.failure("offset:{} can't find offsetIndex!");
        }
        logger.debug("offset:{} find offsetIndex:{} {}", searchKey, offsetPosition.getIndexKey(),offsetPosition.getIndexValue());
        //其实这里无论返回lower 还是upper都行，循环的退出时间是lower==upper。
        return Results.success(offsetPosition);
    }

    private Result<IndexEntry.OffsetPosition> getIndexEntryPositionOffset(long indexPosition) {
        try {
            this.fileReader.position(indexPosition);
            ByteBuffer byteBuffer = ByteBuffer.allocate(DataLogConstants.INDEX_BYTES);
            this.fileReader.read(byteBuffer);
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

}
