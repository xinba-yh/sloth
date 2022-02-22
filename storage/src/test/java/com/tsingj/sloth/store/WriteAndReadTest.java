package com.tsingj.sloth.store;

import com.tsingj.sloth.store.log.LogManager;
import com.tsingj.sloth.store.utils.CrcUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@Slf4j
public class WriteAndReadTest {

    private static final String LOG_NAME = "temp1";
    private static final String LOG_PATH = LOG_NAME + ".data";
    private static final String OFFSET_INDEX_PATH = LOG_NAME + ".index";
    private static final String TIME_INDEX_PATH = LOG_NAME + ".timeindex";

    @Test
    public void writeTest() throws IOException {
        File logFile = new File(LOG_PATH);
        if (logFile.exists()) {
            logFile.delete();
        }
        BufferedOutputStream logWriter = new BufferedOutputStream(new FileOutputStream(logFile, true));
        File indexFile = new File(OFFSET_INDEX_PATH);
        if (indexFile.exists()) {
            indexFile.delete();
        }
        BufferedOutputStream indexWriter = new BufferedOutputStream(new FileOutputStream(indexFile, true));
        File timeIndexFile = new File(TIME_INDEX_PATH);
        if (timeIndexFile.exists()) {
            timeIndexFile.delete();
        }
        BufferedOutputStream timeIndexWriter = new BufferedOutputStream(new FileOutputStream(timeIndexFile, true));

        long offset = 0L;
        long position = 0L;
        for (int i = 0; i < 1000; i++) {
            //写数据
            String data = "i+" + i + ",hello world.";
            byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
            byte[] logBytes = LogManager.buildLog(dataBytes);
            logWriter.write(logBytes);

            //写index  offset -> position
            offset = offset + 1;
            ByteBuffer indexByteBuffer = ByteBuffer.allocate(16);
            indexByteBuffer.putLong(offset);
            indexByteBuffer.putLong(position);
            indexWriter.write(indexByteBuffer.array());

            //写timeindex timestamp -> offset
            ByteBuffer timeIndexByteBuffer = ByteBuffer.allocate(16);
            timeIndexByteBuffer.putLong(System.currentTimeMillis());
            timeIndexByteBuffer.putLong(offset);
            timeIndexWriter.write(timeIndexByteBuffer.array());
            position = position + logBytes.length;
        }

        logWriter.flush();
        indexWriter.flush();
        timeIndexWriter.flush();

        logWriter.close();
        indexWriter.close();
        timeIndexWriter.close();

    }


    @Test
    public void readTest() throws IOException {
        File logFile = new File(LOG_PATH);
        RandomAccessFile logReader = new RandomAccessFile(logFile, "rw");
        File indexFile = new File(OFFSET_INDEX_PATH);
        RandomAccessFile indexReader = new RandomAccessFile(indexFile, "rw");
        File timeIndexFile = new File(TIME_INDEX_PATH);
        RandomAccessFile timeIndexReader = new RandomAccessFile(timeIndexFile, "rw");

        //native方法获取文件大小
        long fileSize = FileUtils.sizeOf(indexFile);
        System.out.println("fileSize:" + fileSize);


        //按照指定offset进行二分查找 查找position 待补充....
        long lo = 0;
        long hi = fileSize;
        long mid = (lo + hi) / 2;
        System.out.println("mid:" + mid);
        indexReader.seek(mid);
        long offset = indexReader.readLong();
        long position = indexReader.readLong();
        System.out.println("offset:" + offset + ",position:" + position);

        //查询消息体

        logReader.seek(position);
        int msgSize = logReader.readInt();
        int version = logReader.read();
        int crc = logReader.readInt();
        byte[] payload = new byte[msgSize];
        logReader.read(payload);
        System.out.println("msgSize:" + msgSize + ",version:" + version + ",crc:" + crc + ",payload:" + new String(payload, StandardCharsets.UTF_8));
        int checkCrc = CrcUtil.crc32(payload);
        System.out.println(crc == checkCrc ? "check success" : "check fail!");


//
//        //读取16字节
//        for (int i = 0; i < 1000; i++) {
//            indexReader.seek(i * 16);
//            long offset = indexReader.readLong();
//            long position = indexReader.readLong();
//            System.out.println("offset:" + offset + ",position:" + position);
//        }


//        ByteBuffer indexByteBuffer = ByteBuffer.wrap(indexByte);
//        long offset = indexByteBuffer.getLong();
//        long position = indexByteBuffer.getLong();
//        System.out.println("offset:" + offset + ",position:" + position);

        logReader.close();
        indexReader.close();
        timeIndexReader.close();
    }
}
