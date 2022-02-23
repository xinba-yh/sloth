package com.tsingj.sloth.store;

import com.tsingj.sloth.store.log.LogManager;
import com.tsingj.sloth.store.utils.CrcUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.springframework.util.StopWatch;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

@Slf4j
public class WriteAndReadSparseIndexOnFileChannelTest {

    private static final String LOG_NAME = "temp1";
    private static final String LOG_PATH = LOG_NAME + ".data";
    private static final String OFFSET_INDEX_PATH = LOG_NAME + ".index";
    private static final String TIME_INDEX_PATH = LOG_NAME + ".timeindex";


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
        for (int i = 0; i < 5000000; i++) {
            offset = offset + 1;
            //写数据
            String data = "i+" + i + ",hello world.";
            byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
            byte[] logBytes = LogManager.buildLog(offset, dataBytes);
            logWriter.write(logBytes);

            //写index  offset -> position
            //稀疏索引方式写入
            if (offset == 1 || offset % 10 == 0) {
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

    /**
     * 1W次读取 929 -> 1000
     * @throws IOException
     */
    @Test
    public void readTestSparseIndex() throws IOException {
        File logFile = new File(LOG_PATH);
        FileChannel logReader = new RandomAccessFile(logFile, "rw").getChannel();
        File indexFile = new File(OFFSET_INDEX_PATH);
        FileChannel indexReader = new RandomAccessFile(indexFile, "rw").getChannel();
        File timeIndexFile = new File(TIME_INDEX_PATH);
        FileChannel timeIndexReader = new RandomAccessFile(timeIndexFile, "rw").getChannel();

        //native方法获取文件大小
        long fileSize = FileUtils.sizeOf(indexFile);
        System.out.println("fileSize:" + fileSize);


        int[] random = random(10000);

        StopWatch sw = new StopWatch();
        //按照指定offset进行二分查找
        for (int i = 1; i <= 10000; i++) {
            sw.start();
            long searchOffset = random[i - 1];
            long startLogPosition = lookUp(searchOffset, fileSize / 16, indexReader);
//            log.info("offset:" + searchOffset + ",startLogPosition:" + startLogPosition);
            if (startLogPosition == -1) {
                log.warn("can not find searchOffset:{}", searchOffset);
                sw.stop();
                continue;
            }
            //查询消息体
            getLogPositionSlotRange(logReader, searchOffset, startLogPosition, FileUtils.sizeOf(logFile));
            sw.stop();
        }
        log.info("" + sw.getTotalTimeMillis());

        logReader.close();
        indexReader.close();
        timeIndexReader.close();
    }

    private long lookUp(long searchOffset, long entries, FileChannel indexReader) throws IOException {
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


    private Long getLogPositionSlotRange(FileChannel logReader, long searchOffset, long startPosition, long maxPosition) throws IOException {
        Long logPosition = null;
        long position = startPosition;
        while (position < maxPosition) {
            //查询消息体
            logReader.position(position);
            ByteBuffer headerByteBuffer = ByteBuffer.allocate(12);
            logReader.read(headerByteBuffer);
            headerByteBuffer.rewind();
            long offset = headerByteBuffer.getLong();
            int msgSize = headerByteBuffer.getInt();
            if (searchOffset == offset) {
                ByteBuffer bodyByteBuffer = ByteBuffer.allocate(LogManager.countMessageBodyBytes(msgSize));
                logReader.read(bodyByteBuffer);
                bodyByteBuffer.rewind();
                byte version = bodyByteBuffer.get();
                int crc = bodyByteBuffer.getInt();
                byte[] payload = new byte[msgSize];
                bodyByteBuffer.get(payload);
//                log.info("searchOffset:" + searchOffset + ",offset:" + offset + ",msgSize:" + msgSize + ",version:" + version + ",crc:" + crc + ",payload:" + new String(payload, StandardCharsets.UTF_8));
                int checkCrc = CrcUtil.crc32(payload);
//                log.info(crc == checkCrc ? "check success" : "check fail!");
                logPosition = position;
                break;
            } else {
                position = position + LogManager.countNextMessagePosition(msgSize);
            }
        }

        if (logPosition == null) {
            log.warn("can not find searchOffset:{} ", searchOffset);
        }
        return logPosition;
    }

    private long getOffsetByPosition(FileChannel indexReader, long position) throws IOException {
        indexReader.position(position);
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        int read = indexReader.read(byteBuffer);
        byteBuffer.rewind();
        return byteBuffer.getLong();
    }

    private long getLogPosition(FileChannel indexReader, long indexPosition) throws IOException {
        indexReader.position(indexPosition + 8);
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        int read = indexReader.read(byteBuffer);
        byteBuffer.rewind();
        return byteBuffer.getLong();
    }


    private int[] random(int num) {
        int i = 1;
        Random random = new Random();
        int[] id = new int[num];
        id[0] = random.nextInt(5000000);
        while (i < num) {
            if (id[i] != random.nextInt(5000000)) {
                id[i] = random.nextInt(5000000);
            } else {
                continue;
            }
            i++;
        }
        Arrays.sort(id);
        System.out.println(Arrays.toString(id));
        return id;
    }
}
