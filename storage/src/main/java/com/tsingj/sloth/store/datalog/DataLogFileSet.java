package com.tsingj.sloth.store.datalog;

import com.tsingj.sloth.store.DataLogConstants;
import com.tsingj.sloth.store.properties.StorageProperties;
import com.tsingj.sloth.store.utils.CommonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author yanghao
 */
@Slf4j
@Component
public class DataLogFileSet {

    private final StorageProperties storageProperties;

    public DataLogFileSet(StorageProperties storageProperties) {
        this.storageProperties = storageProperties;
    }

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
        String logPath = dir + File.separator + fileName + DataLogConstants.FileSuffix.LOG;
        String offsetIndexPath = dir + File.separator + fileName + DataLogConstants.FileSuffix.OFFSET_INDEX;
        String timeIndexPath = dir + File.separator + fileName + DataLogConstants.FileSuffix.TIMESTAMP_INDEX;
        DataLogFile newDataLogFile;
        try {
            newDataLogFile = new DataLogFile(logPath, offsetIndexPath, timeIndexPath, startOffset, storageProperties.getSegmentMaxFileSize(), storageProperties.getLogIndexIntervalBytes());
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

    public List<DataLogFile> getDataLogFiles(String topic, int partition) {
        List<DataLogFile> dataLogFiles = this.DATA_LOGFILE_MAP.get(topic + "_" + partition);
        if (CollectionUtils.isEmpty(dataLogFiles)) {
            return null;
        } else {
            return dataLogFiles;
        }
    }

    public DataLogFile findDataLogFileByOffset(List<DataLogFile> dataLogFiles, long offset) {
        //only one file just return.
        if (dataLogFiles.size() == 1) {
            return dataLogFiles.get(0);
            //if latest file offset lower search offset.
        } else if (dataLogFiles.get(dataLogFiles.size() - 1).getFileFromOffset() < offset) {
            return dataLogFiles.get(dataLogFiles.size() - 1);
        } else {
            //binary search
            List<Long> fileOffsetList = dataLogFiles.parallelStream().map(DataLogFile::getFileFromOffset).collect(Collectors.toList());
            int lower = 0;
            int upper = fileOffsetList.size() - 1;
            while (lower < upper) {
                int mid = (lower + upper + 1) / 2;
                long midValue = fileOffsetList.get(mid);
                if (midValue <= offset) {
                    lower = mid;
                } else {
                    upper = mid - 1;
                }
            }
            return dataLogFiles.get(lower);
//            int finalLower = lower;

//            Optional<DataLogFile> logFileOptional = dataLogFiles.parallelStream().filter(o -> o.getFileFromOffset() == finalLower).findFirst();
//            if(!logFileOptional.isPresent()){
//                System.out.println("find bug!");
//            }
//            return logFileOptional.orElse(null);
        }
    }
}
