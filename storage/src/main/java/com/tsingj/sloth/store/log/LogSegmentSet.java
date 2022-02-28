package com.tsingj.sloth.store.log;

import com.tsingj.sloth.store.properties.StorageProperties;
import com.tsingj.sloth.store.utils.CommonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author yanghao
 */
@Slf4j
@Component
public class LogSegmentSet {

    private final StorageProperties storageProperties;

    public LogSegmentSet(StorageProperties storageProperties) {
        this.storageProperties = storageProperties;
    }

    /**
     * 文件与内存映射。
     */
    private final ConcurrentHashMap<String, List<LogSegment>> DATA_LOGFILE_MAP = new ConcurrentHashMap<>();


    public boolean load() {
        return true;
    }


    public LogSegment getLatestDataLogFile(String topic, int partition) {
        List<LogSegment> logSegments = this.DATA_LOGFILE_MAP.get(topic + "_" + partition);
        if (CollectionUtils.isEmpty(logSegments)) {
            return null;
        } else {
            return logSegments.get(logSegments.size() - 1);
        }
    }

    public LogSegment getLatestDataLogFile(String topic, int partition, long startOffset) {
        //加锁后再次检查，防止多线程重复创建。
        LogSegment latestLogSegment = getLatestDataLogFile(topic, partition);
        if (latestLogSegment != null && !latestLogSegment.isFull()) {
            return latestLogSegment;
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
        String logPath = dir + File.separator + fileName;
//        + DataLogConstants.FileSuffix.LOG;
//        String offsetIndexPath = dir + File.separator + fileName + DataLogConstants.FileSuffix.OFFSET_INDEX;
//        String timeIndexPath = dir + File.separator + fileName + DataLogConstants.FileSuffix.TIMESTAMP_INDEX;
        LogSegment newLogSegment;
        try {
//            newDataLogSegment = new DataLogSegment(logName, startOffset, storageProperties.getSegmentMaxFileSize(), storageProperties.getLogIndexIntervalBytes());
            newLogSegment = new LogSegment(logPath, startOffset, storageProperties.getSegmentMaxFileSize(), storageProperties.getLogIndexIntervalBytes());
        } catch (FileNotFoundException e) {
            return null;
        }
        List<LogSegment> logSegments = this.DATA_LOGFILE_MAP.get(topic + "_" + partition);
        if (logSegments == null) {
            logSegments = new ArrayList<>();
        }
        logSegments.add(newLogSegment);
        this.DATA_LOGFILE_MAP.put(topic + "_" + partition, logSegments);
        return newLogSegment;
    }

    public List<LogSegment> getDataLogFiles(String topic, int partition) {
        List<LogSegment> logSegments = this.DATA_LOGFILE_MAP.get(topic + "_" + partition);
        if (CollectionUtils.isEmpty(logSegments)) {
            return null;
        } else {
            return logSegments;
        }
    }

    public LogSegment findDataLogFileByOffset(List<LogSegment> logSegments, long offset) {
        //only one file just return.
        if (logSegments.size() == 1) {
            return logSegments.get(0);
            //if latest file offset lower search offset.
        } else if (logSegments.get(logSegments.size() - 1).getFileFromOffset() < offset) {
            return logSegments.get(logSegments.size() - 1);
        } else {
            //binary search
            List<Long> fileOffsetList = logSegments.parallelStream().map(LogSegment::getFileFromOffset).collect(Collectors.toList());
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
            LogSegment logSegment = logSegments.get(lower);
            log.debug("offset:{} find DataLogFIle startOffset:{}", offset, logSegment.getFileFromOffset());
            return logSegment;
        }
    }
}
