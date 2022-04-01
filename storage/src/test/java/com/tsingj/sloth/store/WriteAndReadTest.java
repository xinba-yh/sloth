//package com.tsingj.sloth.store;
//
//import com.tsingj.sloth.store.log.DataLogManager;
//import com.tsingj.sloth.store.utils.CrcUtil;
//import org.apache.commons.io.FileUtils;
//import org.junit.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.util.StopWatch;
//
//import java.io.*;
//import java.nio.ByteBuffer;
//import java.nio.charset.StandardCharsets;
//
//public class WriteAndReadTest {
//
//    private static final Logger log = LoggerFactory.getLogger(WriteAndReadTest.class);
//
//    private static final String LOG_NAME = "temp1";
//    private static final String LOG_PATH = LOG_NAME + ".data";
//    private static final String OFFSET_INDEX_PATH = LOG_NAME + ".index";
//    private static final String TIME_INDEX_PATH = LOG_NAME + ".timeindex";
//
//    @Test
//    public void writeTest() throws IOException {
//        File logFile = new File(LOG_PATH);
//        if (logFile.exists()) {
//            logFile.delete();
//        }
//        BufferedOutputStream logWriter = new BufferedOutputStream(new FileOutputStream(logFile, true));
//        File indexFile = new File(OFFSET_INDEX_PATH);
//        if (indexFile.exists()) {
//            indexFile.delete();
//        }
//        BufferedOutputStream indexWriter = new BufferedOutputStream(new FileOutputStream(indexFile, true));
//        File timeIndexFile = new File(TIME_INDEX_PATH);
//        if (timeIndexFile.exists()) {
//            timeIndexFile.delete();
//        }
//        BufferedOutputStream timeIndexWriter = new BufferedOutputStream(new FileOutputStream(timeIndexFile, true));
//
//        long offset = 0L;
//        long position = 0L;
//        for (int i = 0; i < 1000; i++) {
//
//            offset = offset + 1;
//            //写数据
//            String data = "i+" + i + ",hello world.";
//            byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
//            byte[] logBytes = DataLogManager.buildLog(offset, dataBytes);
//            logWriter.write(logBytes);
//
//            //写index  offset -> position
//            ByteBuffer indexByteBuffer = ByteBuffer.allocate(16);
//            indexByteBuffer.putLong(offset);
//            indexByteBuffer.putLong(position);
//            indexWriter.write(indexByteBuffer.array());
//
//            //写timeindex timestamp -> offset
//            ByteBuffer timeIndexByteBuffer = ByteBuffer.allocate(16);
//            timeIndexByteBuffer.putLong(SystemClock.now()());
//            timeIndexByteBuffer.putLong(offset);
//            timeIndexWriter.write(timeIndexByteBuffer.array());
//            position = position + logBytes.length;
//        }
//
//        logWriter.flush();
//        indexWriter.flush();
//        timeIndexWriter.flush();
//
//        logWriter.close();
//        indexWriter.close();
//        timeIndexWriter.close();
//
//    }
//
//
//    @Test
//    public void readTest() throws IOException {
//        File logFile = new File(LOG_PATH);
//        RandomAccessFile logReader = new RandomAccessFile(logFile, "rw");
//        File indexFile = new File(OFFSET_INDEX_PATH);
//        RandomAccessFile indexReader = new RandomAccessFile(indexFile, "rw");
//        File timeIndexFile = new File(TIME_INDEX_PATH);
//        RandomAccessFile timeIndexReader = new RandomAccessFile(timeIndexFile, "rw");
//
//        //native方法获取文件大小
//        long fileSize = FileUtils.sizeOf(indexFile);
//        System.out.println("fileSize:" + fileSize);
//
//        StopWatch sw = new StopWatch();
//        //按照指定offset进行二分查找
//        for (int i = 1; i <= 1000; i++) {
//            sw.start();
//            long searchOffset = i;
//            long logPosition = getLogPosition(indexReader, (searchOffset - 1) * 16L);
//            log.info("offset:" + searchOffset + ",logPosition:" + logPosition);
//
//            //消息获取
//            logReader.seek(logPosition);
//            long offset = logReader.readLong();
//            int msgSize = logReader.readInt();
//            int version = logReader.read();
//            int crc = logReader.readInt();
//            byte[] payload = new byte[msgSize];
//            logReader.read(payload);
//            log.info("offset:" + offset + ",msgSize:" + msgSize + ",version:" + version + ",crc:" + crc + ",payload:" + new String(payload, StandardCharsets.UTF_8));
//            int checkCrc = CrcUtil.crc32(payload);
//            log.info(crc == checkCrc ? "check success" : "check fail!");
//            sw.stop();
//        }
//        log.info("" + sw.getTotalTimeMillis());
//
//        logReader.close();
//        indexReader.close();
//        timeIndexReader.close();
//    }
//
//
//    private long getLogPosition(RandomAccessFile indexReader, long indexPosition) throws IOException {
//        indexReader.seek(indexPosition + 8);
//        return indexReader.readLong();
//    }
//
//}
