package com.tsingj.sloth.store.log;

import com.tsingj.sloth.store.DataLogConstants;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author yanghao
 */
public class TimeIndex {

    /**
     * offsetIndex物理文件
     */
    private File file;

    private FileChannel fileReader;

    private BufferedOutputStream fileWriter;

    public TimeIndex(String logPath) throws FileNotFoundException {
        //init offsetIndex file operator
        file = new File(logPath + DataLogConstants.FileSuffix.TIMESTAMP_INDEX);
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

}
