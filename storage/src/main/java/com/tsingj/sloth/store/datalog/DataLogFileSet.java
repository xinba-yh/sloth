package com.tsingj.sloth.store.datalog;

import com.tsingj.sloth.store.properties.StorageProperties;
import com.tsingj.sloth.store.utils.CommonUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yanghao
 */
@Component
public class DataLogFileSet {


    @Autowired
    private StorageProperties storageProperties;

    /**
     * 文件与内存映射。
     */
    private final ConcurrentHashMap<String, List<DataLogFile>> DATA_LOGFILE_MAP = new ConcurrentHashMap<>();

    /**
     * 获取最新的DataLogFile
     * //     * ---- 上层appendMessage会有锁
     *
     * @param topic
     * @param needLen
     * @param partition
     * @return
     */
    public DataLogFile getLatestDataLogFile(String topic, int partition, int needLen) throws FileNotFoundException {
        List<DataLogFile> dataLogFiles = this.DATA_LOGFILE_MAP.get(topic + "_" + partition);
        if (CollectionUtils.isEmpty(dataLogFiles)) {
            String topicPartitionDirPath = "data" + File.separator + topic + File.separator + partition;
            File dir = new File(topicPartitionDirPath);
            if (!dir.exists()) {
                boolean mkdirs = dir.mkdirs();
                Assert.isTrue(mkdirs, "File dir not exists,but create fail!");
            }
            String fileName = CommonUtil.offset2FileName(0);
            String logPath = dir + File.separator + fileName + ".data";
            String offsetIndexPath = dir + File.separator + fileName + ".index";
            String timeIndexPath = dir + File.separator + fileName + ".timeindex";
            DataLogFile dataLogFile = new DataLogFile(logPath, offsetIndexPath, timeIndexPath, storageProperties.getMessageMaxSize());
            dataLogFiles = new ArrayList<>();
            dataLogFiles.add(dataLogFile);
            this.DATA_LOGFILE_MAP.put(topic + "_" + partition, dataLogFiles);
            return dataLogFile;
        } else {
            DataLogFile dataLogFile = dataLogFiles.get(dataLogFiles.size() - 1);
            boolean full = dataLogFile.isFull(needLen);
            if (full) {
                String topicPartitionDirPath = "data" + File.separator + topic + File.separator + partition;
                File dir = new File(topicPartitionDirPath);
                String fileName = CommonUtil.offset2FileName(dataLogFile.getCurrentOffset() + 1);
                String logPath = dir + File.separator + fileName + ".data";
                String offsetIndexPath = dir + File.separator + fileName + ".index";
                String timeIndexPath = dir + File.separator + fileName + ".timeindex";
                DataLogFile newDataLogFile = new DataLogFile(logPath, offsetIndexPath, timeIndexPath, storageProperties.getMessageMaxSize());
                dataLogFiles = new ArrayList<>();
                dataLogFiles.add(dataLogFile);
                this.DATA_LOGFILE_MAP.put(topic + "_" + partition, dataLogFiles);
                return newDataLogFile;
            } else {
                return dataLogFile;
            }
        }
    }

    public boolean load() {
        return true;
    }


}
