package com.tsingj.sloth.store.datalog;

import com.tsingj.sloth.store.properties.StorageProperties;
import com.tsingj.sloth.store.utils.CommonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yanghao
 */
@Slf4j
@Component
public class DataLogFileSet {


    @Autowired
    private StorageProperties storageProperties;

    /**
     * 文件与内存映射。
     */
    private final ConcurrentHashMap<String, List<DataLogFile>> DATA_LOGFILE_MAP = new ConcurrentHashMap<>();


    public boolean load() {
        return true;
    }


    public DataLogFile getLatestDataLogFile(String topic, int partition) {
        List<DataLogFile> dataLogFiles = this.DATA_LOGFILE_MAP.get(topic + "_" + partition);
        if (CollectionUtils.isEmpty(dataLogFiles)) {
            return null;
        } else {
            return dataLogFiles.get(dataLogFiles.size() - 1);
        }
    }

    public DataLogFile getLatestDataLogFile(String topic, int partition, long startOffset) {
        //加锁后再次检查，防止多线程重复创建。
        DataLogFile latestDataLogFile = getLatestDataLogFile(topic, partition);
        if (latestDataLogFile != null && !latestDataLogFile.isFull()) {
            return latestDataLogFile;
        }

        //创建文件
        String topicPartitionDirPath = "data" + File.separator + topic + File.separator + partition;
        File dir = new File(topicPartitionDirPath);
        if (!dir.exists()) {
            boolean mkdirs = dir.mkdirs();
            if (mkdirs) {
                log.info("create datalog dir {} success.", dir);
            } else {
                log.error("datalog dir make fail!");
                return null;
            }
        }
        String fileName = CommonUtil.offset2FileName(startOffset);
        String logPath = dir + File.separator + fileName + ".data";
        String offsetIndexPath = dir + File.separator + fileName + ".index";
        String timeIndexPath = dir + File.separator + fileName + ".timeindex";
        DataLogFile newDataLogFile;
        try {
            newDataLogFile = new DataLogFile(logPath, offsetIndexPath, timeIndexPath, startOffset, storageProperties.getMessageMaxSize());
        } catch (FileNotFoundException e) {
            return null;
        }
        List<DataLogFile> dataLogFiles = this.DATA_LOGFILE_MAP.get(topic + "_" + partition);
        if (dataLogFiles == null) {
            dataLogFiles = new ArrayList<>();
        }
        dataLogFiles.add(newDataLogFile);
        this.DATA_LOGFILE_MAP.put(topic + "_" + partition, dataLogFiles);
        return newDataLogFile;
    }

}
