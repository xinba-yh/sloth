package com.tsingj.sloth.store;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

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
     * 1000W 1S 20W
     * @throws IOException
     */
    @Test
    void bufferedOutPutStreamTest() throws IOException {
        String message = CompressUtil.GZIP.compressWithBytes(JSON.toJSONString(Message.builder().id(1L).data(simple_data).createTime(System.currentTimeMillis()).build()).getBytes(StandardCharsets.UTF_8));
        BufferedOutputStream bouput = new BufferedOutputStream(
                new FileOutputStream("temp4.data",true));
        long start2 = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            bouput.write((i+","+message+"\n").getBytes(StandardCharsets.UTF_8));
            bouput.flush();
        }
        bouput.close();
        long stop2 = System.currentTimeMillis();
        long time2 = stop2-start2;
        System.out.println("BufferedWriter的时间差为："+ time2 +" 毫秒");
    }

}
