package com.tsingj.sloth.store;

import com.alibaba.fastjson.JSON;
import com.tsingj.sloth.store.utils.CompressUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.springframework.util.StopWatch;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

@Slf4j
class StorageApplicationTests {

    private static final String topic = "test";
    private static final Integer partition = 8;
    private static final List<Integer> partitions = Arrays.asList(0);


    /**
     * 8112 bytes
     */
    private static final String simple_data = "message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info message info  info";

    @Test
    void dataLen() {
        System.out.println(simple_data.getBytes(StandardCharsets.UTF_8).length);
    }

    @Test
    void bufferedWriterTest() {
        String message = CompressUtil.GZIP.compressWithBytes(JSON.toJSONString(Message.builder().id(1L).data(simple_data).createTime(System.currentTimeMillis()).build()).getBytes(StandardCharsets.UTF_8));
        String topicPartitionDirPath = "data" + File.separator + topic + File.separator + partition;
        File dir = new File(topicPartitionDirPath);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        File file = new File(topicPartitionDirPath + File.separator + "segment_" + 0 + ".data");
        long startTime = System.currentTimeMillis();
        BufferedWriter bufferedWriter = null;
        for (int i = 0; i < 100000; i++) {
            try {
                bufferedWriter = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
                bufferedWriter.append(message).append("\n");
                bufferedWriter.flush();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (bufferedWriter != null) {
                        bufferedWriter.close();
                    }
                } catch (IOException ignored) {
                }
            }
        }
        System.out.println(System.currentTimeMillis() - startTime);
    }


    @Test
    void commonIoFileWriterTest() {
        String message = CompressUtil.GZIP.compressWithBytes(JSON.toJSONString(Message.builder().id(1L).data(simple_data).createTime(System.currentTimeMillis()).build()).getBytes(StandardCharsets.UTF_8));
        String topicPartitionDirPath = "data" + File.separator + topic + File.separator + partition;
        File dir = new File(topicPartitionDirPath);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        File file = new File(topicPartitionDirPath + File.separator + "segment_" + 0 + ".data");
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            try {
                FileUtils.write(file, message, Charset.defaultCharset(), true);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {

            }
        }
        System.out.println(System.currentTimeMillis() - startTime);
    }

    /**
     * 1000W 1S 25W
     *
     * @throws IOException
     */
    @Test
    void bufferedOutPutStreamTest() throws IOException {
        String message = CompressUtil.GZIP.compressWithBytes(JSON.toJSONString(Message.builder().id(1L).data(simple_data).createTime(System.currentTimeMillis()).build()).getBytes(StandardCharsets.UTF_8));
        BufferedOutputStream bouput = new BufferedOutputStream(
                new FileOutputStream("temp4.data", true));
        long start2 = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            bouput.write((i + "," + message + "\n").getBytes(StandardCharsets.UTF_8));
            bouput.flush();
        }
        bouput.close();
        long stop2 = System.currentTimeMillis();
        long time2 = stop2 - start2;
        System.out.println("BufferedWriter的时间差为：" + time2 + " 毫秒");
    }


    /**
     * 1000W 5S  1S 200W
     *
     * @throws IOException
     */
    @Test
    void bufferedOutPutStreamEndFlushTest() throws IOException {
        String message = CompressUtil.GZIP.compressWithBytes(JSON.toJSONString(Message.builder().id(1L).data(simple_data).createTime(System.currentTimeMillis()).build()).getBytes(StandardCharsets.UTF_8));
        BufferedOutputStream bouput = new BufferedOutputStream(
                new FileOutputStream("temp5.data", true));
        long start2 = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            bouput.write((i + "," + message + "\n").getBytes(StandardCharsets.UTF_8));
        }
        bouput.flush();
        bouput.close();
        long stop2 = System.currentTimeMillis();
        long time2 = stop2 - start2;
        System.out.println("BufferedWriter的时间差为：" + time2 + " 毫秒");
    }


    @Test
    void bufferedOutPutStreamCountFlushTest() throws IOException {
        /**
         * write process
         * 800W 4S
         */
        String message = CompressUtil.GZIP.compressWithBytes(JSON.toJSONString(Message.builder().id(1L).data(simple_data).createTime(System.currentTimeMillis()).build()).getBytes(StandardCharsets.UTF_8));
        File file = new File("temp6.data");
        if (file.exists()) {
            file.delete();
        }
        BufferedOutputStream bouput = new BufferedOutputStream(
                new FileOutputStream(file, true));
        long start2 = System.currentTimeMillis();
        long flushTime = System.currentTimeMillis();
        int offset = 0;
        int selectOffset = 0;
        int selectBytes = 0;
        for (int i = 0; i < 8000000; i++) {
            byte[] data = (i + "@" + message).getBytes(StandardCharsets.UTF_8);
            bouput.write(data);
            if (System.currentTimeMillis() - flushTime > 200) {
                //内存 -> 磁盘 刷盘
                bouput.flush();
                flushTime = System.currentTimeMillis();
            }
            offset = offset + data.length;
            if (i == 7000001) {
                selectOffset = offset - data.length;
                selectBytes = data.length;
            }
        }
        bouput.close();
        long stop2 = System.currentTimeMillis();
        long time2 = stop2 - start2;
        System.out.println("BufferedWriter的时间差为：" + time2 + " 毫秒");


        /**
         * read process
         * 10W --  225
         */
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        StopWatch sw = new StopWatch();
        byte[] messageBytes = new byte[selectBytes];
        for (int i = 0; i < 100000; i++) {
            sw.start();
            raf.seek(selectOffset);
            raf.read(messageBytes);
            sw.stop();
        }
        System.out.println(sw.getTaskCount() + " --  " + sw.getTotalTimeMillis());

        String messageString = new String(messageBytes, StandardCharsets.UTF_8);
        String[] messageArr = messageString.split("@");
        System.out.println("index:" + messageArr[0]);
        byte[] bytes = CompressUtil.GZIP.uncompressRespBytes(messageArr[1]);
        String messageJson = JSON.toJSONString(new String(bytes, StandardCharsets.UTF_8));
        System.out.println("body:" + messageJson);


    }

    @Test
    void mappedByteBufferWriteTest() throws IOException {
        /**
         * write process
         * 800W 4S
         */
        String message = CompressUtil.GZIP.compressWithBytes(JSON.toJSONString(Message.builder().id(1L).data(simple_data).createTime(System.currentTimeMillis()).build()).getBytes(StandardCharsets.UTF_8));
        File file = new File("temp7.data");
        if (file.exists()) {
            file.delete();
        }
        FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
        //1G
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 1024 * 1024 * 1024);
        long start2 = System.currentTimeMillis();
        long flushTime = System.currentTimeMillis();
        int offset = 0;
        int selectOffset = 0;
        int selectBytes = 0;
        for (int i = 0; i < 8000000; i++) {
            byte[] data = (i + "@" + message).getBytes(StandardCharsets.UTF_8);
            mappedByteBuffer.put(data);
            if (System.currentTimeMillis() - flushTime > 200) {
                //内存-> 磁盘 刷盘
                mappedByteBuffer.force();
                flushTime = System.currentTimeMillis();
            }
            offset = offset + data.length;
            if (i == 7000001) {
                selectOffset = offset - data.length;
                selectBytes = data.length;
            }
        }
        //内存-> 磁盘 刷盘
        mappedByteBuffer.force();
        long stop2 = System.currentTimeMillis();
        long time2 = stop2 - start2;
        System.out.println("mappedByteBufferTest的时间差为：" + time2 + " 毫秒");


        StopWatch sw = new StopWatch();
        /**
         * read process
         * 10W 10ms
         */
        byte[] messageBytes = new byte[selectBytes];
        for (int i = 0; i < 100000; i++) {
            sw.start();
            mappedByteBuffer.position(selectOffset);
            mappedByteBuffer.get(messageBytes);
            sw.stop();
        }
        System.out.println(sw.getTaskCount() + " --  " + sw.getTotalTimeMillis());

        String messageString = new String(messageBytes, StandardCharsets.UTF_8);
        String[] messageArr = messageString.split("@");
        System.out.println("index:" + messageArr[0]);
        byte[] bytes = CompressUtil.GZIP.uncompressRespBytes(messageArr[1]);
        String messageJson = JSON.toJSONString(new String(bytes, StandardCharsets.UTF_8));
        System.out.println("body:" + messageJson);


        //继续写入 查看是否正确写入
        byte[] data = (8000001 + "@" + message).getBytes(StandardCharsets.UTF_8);
        mappedByteBuffer.position(offset);
        mappedByteBuffer.put(data);
        mappedByteBuffer.force();

        //继续读取
        messageBytes = new byte[data.length];
        mappedByteBuffer.position(offset);
        mappedByteBuffer.get(messageBytes);

        messageString = new String(messageBytes, StandardCharsets.UTF_8);
        messageArr = messageString.split("@");
        System.out.println("index:" + messageArr[0]);
        bytes = CompressUtil.GZIP.uncompressRespBytes(messageArr[1]);
        messageJson = JSON.toJSONString(new String(bytes, StandardCharsets.UTF_8));
        System.out.println("body:" + messageJson);

        fileChannel.close();
    }

}
