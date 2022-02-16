package com.tsingj.sloth.store;

import com.tsingj.sloth.store.properties.StorageProperties;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;

/**
 * @author yanghao
 */
public class DefaultStorageEngine implements StorageService {

//    @Autowired
//    private StorageProperties storageProperties;

    @Override
    public void write(String topic, int partition, String data) {
        String topicPartitionDirPath = "data" + File.separator + topic + File.separator + partition;
        File dir = new File(topicPartitionDirPath);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        File file = new File(topicPartitionDirPath + File.separator + "segment_" + 0 + ".data");
        try {
            FileUtils.write(file, data, Charset.defaultCharset(), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
