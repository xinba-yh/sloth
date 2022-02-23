package com.tsingj.sloth.store;

import com.tsingj.sloth.store.log.LogManager;
import com.tsingj.sloth.store.utils.CrcUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.springframework.util.StopWatch;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

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

            offset = offset + 1;
            //写数据
            String data = "i+" + i + ",hello world.";
            byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
            byte[] logBytes = LogManager.buildLog(offset, dataBytes);
            logWriter.write(logBytes);

            //写index  offset -> position
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

        StopWatch sw = new StopWatch();
        //按照指定offset进行二分查找
        for (int i = 1; i <= 1000; i++) {
            sw.start();
            long searchOffset = i;
            long logPosition = getLogPosition(indexReader, (searchOffset - 1) * 16L);
            log.info("offset:" + searchOffset + ",logPosition:" + logPosition);

            //消息获取
            logReader.seek(logPosition);
            long offset = logReader.readLong();
            int msgSize = logReader.readInt();
            int version = logReader.read();
            int crc = logReader.readInt();
            byte[] payload = new byte[msgSize];
            logReader.read(payload);
            log.info("offset:" + offset + ",msgSize:" + msgSize + ",version:" + version + ",crc:" + crc + ",payload:" + new String(payload, StandardCharsets.UTF_8));
            int checkCrc = CrcUtil.crc32(payload);
            log.info(crc == checkCrc ? "check success" : "check fail!");
            sw.stop();
        }
        log.info("" + sw.getTotalTimeMillis());

        logReader.close();
        indexReader.close();
        timeIndexReader.close();
    }


    @Test
    public void writeTestSparseIndex() throws IOException {
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
        List<Integer> offsetIndexArr = Arrays.asList(1, 16, 50, 61, 62, 62, 68, 77, 81, 103, 109, 123, 128, 130, 133, 144, 152, 189, 193, 199, 199, 209, 213, 235, 237, 255, 288, 303, 303, 312, 313, 328, 332, 342, 347, 349, 351, 375, 397, 414, 430, 441, 463, 488, 489, 492, 493, 498, 517, 519, 535, 560, 565, 568, 583, 591, 594, 599, 604, 611, 615, 615, 629, 629, 634, 636, 665, 680, 684, 687, 687, 688, 703, 712, 718, 724, 727, 728, 741, 758, 776, 789, 805, 808, 824, 828, 838, 863, 863, 873, 884, 889, 896, 902, 912, 933, 951, 965, 966, 971, 974);
        for (int i = 0; i < 1000; i++) {
            offset = offset + 1;
            //写数据
            String data = "i+" + i + ",hello world.";
            byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
            byte[] logBytes = LogManager.buildLog(offset, dataBytes);
            logWriter.write(logBytes);

            //写index  offset -> position
            //稀疏索引方式写入
            if (offsetIndexArr.contains(i + 1)) {
                ByteBuffer indexByteBuffer = ByteBuffer.allocate(16);
                indexByteBuffer.putLong(offset);
                indexByteBuffer.putLong(position);
                indexWriter.write(indexByteBuffer.array());

                //写timeindex timestamp -> offset
                ByteBuffer timeIndexByteBuffer = ByteBuffer.allocate(16);
                timeIndexByteBuffer.putLong(System.currentTimeMillis());
                timeIndexByteBuffer.putLong(offset);
                timeIndexWriter.write(timeIndexByteBuffer.array());
            }

            //增加物理位移量
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
    public void readTestSparseIndex() throws IOException {
        File logFile = new File(LOG_PATH);
        RandomAccessFile logReader = new RandomAccessFile(logFile, "rw");
        File indexFile = new File(OFFSET_INDEX_PATH);
        RandomAccessFile indexReader = new RandomAccessFile(indexFile, "rw");
        File timeIndexFile = new File(TIME_INDEX_PATH);
        RandomAccessFile timeIndexReader = new RandomAccessFile(timeIndexFile, "rw");

        //native方法获取文件大小
        long fileSize = FileUtils.sizeOf(indexFile);
        System.out.println("fileSize:" + fileSize);


        StopWatch sw = new StopWatch();
        //按照指定offset进行二分查找
        for (int i = 1; i <= 1000; i++) {
            sw.start();
            long searchOffset = i;
            long startLogPosition = lookUp(searchOffset, fileSize / 16, indexReader);
//            log.info("offset:" + searchOffset + ",startLogPosition:" + startLogPosition);
            if (startLogPosition == -1) {
                log.warn("can not find searchOffset:{}", searchOffset);
                sw.stop();
                continue;
            }
            sw.stop();
            //查询消息体
            getLogPositionSlotRange(logReader, searchOffset, startLogPosition, FileUtils.sizeOf(logFile));

        }
        log.info("" + sw.getTotalTimeMillis());

        logReader.close();
        indexReader.close();
        timeIndexReader.close();
    }

    private long lookUp(long searchOffset, long entries, RandomAccessFile indexReader) throws IOException {
        //index为空，返回-1
        if (entries == 0) {
            return -1;
        }
        //最小值大与查询值，从头找。  PS:务必将第一条索引插入。
        long startOffset = getOffsetByPosition(indexReader, 0);
        if (startOffset > searchOffset) {
            return -1;
        }
        //开始二分查找 <= searchOffset的最大值
        long lower = 0L;
        long upper = entries - 1;
        while (lower < upper) {
            //这样的操作是为了让 mid 标志 取高位，否则会出现死循环
            long mid = (lower + upper + 1) / 2;
            long found = getOffsetByPosition(indexReader, mid * 16);
            if (found <= searchOffset) { //因为a[mid]<=k,所以a[mid]可能=k，所以mid坐标也满足条件，l = mid而不是mid+1;
                lower = mid;
            } else {//这是a[mid] > k的时候。
                upper = mid - 1;
            }
        }
        //其实这里无论返回lower 还是upper都行，循环的退出时间是lower==upper。

//        log.info("searchOffset:{}，slotOffset:{}", searchOffset, lower);
        //find logPosition slot range
        long startPosition = lower * 16;
        return getLogPosition(indexReader, startPosition);
    }

    private long getLogPosition(RandomAccessFile indexReader, long indexPosition) throws IOException {
        indexReader.seek(indexPosition + 8);
        return indexReader.readLong();
    }

    private Long getLogPositionSlotRange(RandomAccessFile logReader, long searchOffset, long startPosition, long maxPosition) throws IOException {
        Long logPosition = null;
        long position = startPosition;
        while (position < maxPosition) {
            //查询消息体
            logReader.seek(position);
            long offset = logReader.readLong();
            if (searchOffset == offset) {
                int msgSize = logReader.readInt();
                int version = logReader.read();
                int crc = logReader.readInt();
                byte[] payload = new byte[msgSize];
                logReader.read(payload);
                log.info("searchOffset:" + searchOffset + ",offset:" + offset + ",msgSize:" + msgSize + ",version:" + version + ",crc:" + crc + ",payload:" + new String(payload, StandardCharsets.UTF_8));
                int checkCrc = CrcUtil.crc32(payload);
//                log.info(crc == checkCrc ? "check success" : "check fail!");

                logPosition = position;
                break;
            } else {
                int msgSize = logReader.readInt();
                position = position + LogManager.countNextMessagePosition(msgSize);
            }
        }

        if (logPosition == null) {
            log.warn("can not find searchOffset:{} ", searchOffset);
        }
        return logPosition;
    }

    private long getOffsetByPosition(RandomAccessFile indexReader, long position) throws IOException {
        indexReader.seek(position);
        return indexReader.readLong();
    }

    @Test
    public void random() {
        int max = 100;
        int i = 1;
        Random random = new Random();
        int[] id = new int[max];
        id[0] = random.nextInt(1000);
        while (i < max) {
            if (id[i] != random.nextInt(1000)) {
                id[i] = random.nextInt(1000);
            } else {
                continue;
            }
            i++;
        }
        StringBuilder sb = new StringBuilder();
        Arrays.sort(id);
        for (int j : id) {
            sb.append(j).append(",");
        }
        System.out.println(sb);
    }
}
