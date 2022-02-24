package com.tsingj.sloth.store.datalog;

import com.tsingj.sloth.store.Message;
import com.tsingj.sloth.store.utils.CommonUtil;
import org.apache.commons.io.FileUtils;

import java.io.*;

/**
 * @author yanghao
 */
public class DataLogFile {

    /**
     * 文件开始offset
     */
    private long fileFromOffset;

    /**
     * 当前offset
     */

    private long currentOffset;

    private long maxFileSize;

    //log物理文件
    private File file;

    private BufferedOutputStream logWriter;

    //offsetIndex物理文件
    private File offsetIndexFile;

    private BufferedOutputStream offsetIndexWriter;

    //timeIndex物理文件
    private File timeIndexFile;

    private BufferedOutputStream timeIndexWriter;


    public DataLogFile() {

    }

    public DataLogFile(String logPath, String offsetIndexPath, String timeIndexPath, int maxFileSize) throws FileNotFoundException {
        this.firstInit(logPath, offsetIndexPath, timeIndexPath, maxFileSize);
    }

    private void firstInit(String logPath, String offsetIndexPath, String timeIndexPath, int maxFileSize) throws FileNotFoundException {
        this.logWriter = new BufferedOutputStream(new FileOutputStream(logPath, true));
        this.offsetIndexWriter = new BufferedOutputStream(new FileOutputStream(offsetIndexPath, true));
        this.timeIndexWriter = new BufferedOutputStream(new FileOutputStream(timeIndexPath, true));

        this.maxFileSize = maxFileSize;
        this.currentOffset = 0;
        this.fileFromOffset = 0;
    }

    public boolean isFull() {
        return FileUtils.sizeOf(file) == maxFileSize;
    }

    public boolean isFull(int needLen) {
        return (FileUtils.sizeOf(file) + needLen) > maxFileSize;
    }

    public long getCurrentOffset() {
        return this.currentOffset;
    }

    public void doAppend(byte[] data) throws IOException {
        this.logWriter.write(data);
    }
}
