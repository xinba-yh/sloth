package com.tsingj.sloth.store.log;

import com.tsingj.sloth.store.properties.StorageProperties;
import com.tsingj.sloth.store.utils.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yanghao
 */
@Component
public class LogSegmentSet {

    private static final Logger logger = LoggerFactory.getLogger(LogSegmentSet.class);

    private final StorageProperties storageProperties;

    public LogSegmentSet(StorageProperties storageProperties) {
        this.storageProperties = storageProperties;
    }

    /**
     * 文件与内存映射。
     */
    protected final ConcurrentHashMap<String, List<LogSegment>> DATA_LOGFILE_MAP = new ConcurrentHashMap<>();


    public LogSegment getLatestLogSegmentFile(String topic, int partition) {
        List<LogSegment> logSegments = this.DATA_LOGFILE_MAP.get(topic + "_" + partition);
        if (CollectionUtils.isEmpty(logSegments)) {
            return null;
        } else {
            return logSegments.get(logSegments.size() - 1);
        }
    }

    public LogSegment getLatestLogSegmentFile(String topic, int partition, long startOffset) {
        //加锁后再次检查，防止多线程重复创建。
        LogSegment latestLogSegment = getLatestLogSegmentFile(topic, partition);
        if (latestLogSegment != null && !latestLogSegment.isFull()) {
            return latestLogSegment;
        }

        //创建文件
        String topicPartitionDirPath = storageProperties.getDataPath() + File.separator + topic + File.separator + partition;
        File dir = new File(topicPartitionDirPath);
        if (!dir.exists()) {
            boolean mkdirs = dir.mkdirs();
            if (mkdirs) {
                logger.info("create datalog dir {} success.", dir);
            } else {
                throw new RuntimeException("datalog dir make fail!");
            }
        }
        String fileName = CommonUtil.offset2FileName(startOffset);
        String logPath = dir + File.separator + fileName;
        LogSegment newLogSegment;
        try {
            newLogSegment = new LogSegment(logPath, startOffset, storageProperties.getSegmentMaxFileSize(), storageProperties.getLogIndexIntervalBytes());
        } catch (FileNotFoundException e) {
            return null;
        }
        this.addLogSegment(topic, partition, newLogSegment);
        return newLogSegment;
    }

    public List<LogSegment> getLogSegments(String topic, int partition) {
        List<LogSegment> logSegments = this.DATA_LOGFILE_MAP.get(topic + "_" + partition);
        if (CollectionUtils.isEmpty(logSegments)) {
            return null;
        } else {
            return logSegments;
        }
    }

    public LogSegment findLogSegmentByOffset(List<LogSegment> logSegments, long offset) {
        Optional<LogSegment> firstLtSearchOffsetOptional = logSegments.parallelStream().sorted(Comparator.comparing(LogSegment::getFileFromOffset, Comparator.reverseOrder())).filter(o -> o.getFileFromOffset() <= offset).findFirst();
        if (!firstLtSearchOffsetOptional.isPresent()) {
            return null;
        }
        LogSegment logSegment = firstLtSearchOffsetOptional.get();
        logger.debug("offset:{} find logSegment startOffset:{}", offset, logSegment.getFileFromOffset());
        return logSegment;
    }

    protected void addLogSegment(String topic, long partition, LogSegment logSegment) {
        List<LogSegment> logSegments = this.DATA_LOGFILE_MAP.get(topic + "_" + partition);
        if (logSegments == null) {
            logSegments = new ArrayList<>();
        }
        logSegments.add(logSegment);
        this.DATA_LOGFILE_MAP.put(topic + "_" + partition, logSegments);
    }

    public Map<String, List<LogSegment>> getLogSegmentsMapping() {
        return this.DATA_LOGFILE_MAP;
    }
}
