package com.tsingj.sloth.store;

import com.tsingj.sloth.store.datalog.DataLogManager;
import com.tsingj.sloth.store.utils.CrcUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.springframework.util.StopWatch;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

@Slf4j
public class WriteAndReadSparseIndexOnRandomAccessTest {

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
        RandomAccessFile logWriter = new RandomAccessFile(logFile, "rw");
        File indexFile = new File(OFFSET_INDEX_PATH);
        if (indexFile.exists()) {
            indexFile.delete();
        }
        RandomAccessFile indexWriter = new RandomAccessFile(indexFile, "rw");
        File timeIndexFile = new File(TIME_INDEX_PATH);
        if (timeIndexFile.exists()) {
            timeIndexFile.delete();
        }

        RandomAccessFile timeIndexWriter = new RandomAccessFile(timeIndexFile, "rw");
        long offset = 0L;
        long position = 0L;

        for (int i = 0; i < 500000 * 5; i++) {
            offset = offset + 1;
            //写数据
            String data = "i+" + i + ",hello world.";
            byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
            byte[] logBytes = DataLogManager.buildLog(offset, dataBytes);
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

        logWriter.close();
        indexWriter.close();
        timeIndexWriter.close();

    }

    /**
     * 1W -> 4082
     *
     * @throws IOException
     */
    @Test
    public void readTestSparseIndex() throws IOException {
        File logFile = new File(LOG_PATH);
        RandomAccessFile logReader = new RandomAccessFile(logFile, "rws");
        File indexFile = new File(OFFSET_INDEX_PATH);
        RandomAccessFile indexReader = new RandomAccessFile(indexFile, "rws");
        File timeIndexFile = new File(TIME_INDEX_PATH);
        RandomAccessFile timeIndexReader = new RandomAccessFile(timeIndexFile, "rws");

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
                byte version = logReader.readByte();
                int crc = logReader.readInt();
                byte[] payload = new byte[msgSize];
                logReader.read(payload);
//                log.info("searchOffset:" + searchOffset + ",offset:" + offset + ",msgSize:" + msgSize + ",version:" + version + ",crc:" + crc + ",payload:" + new String(payload, StandardCharsets.UTF_8));
                int checkCrc = CrcUtil.crc32(payload);
//                log.info(crc == checkCrc ? "check success" : "check fail!");
                logPosition = position;
                break;
            } else {
                int msgSize = logReader.readInt();
                position = position + DataLogManager.countNextMessagePosition(msgSize);
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
