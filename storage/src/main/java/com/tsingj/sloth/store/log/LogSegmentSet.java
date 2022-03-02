package com.tsingj.sloth.store.log;


import com.tsingj.sloth.store.properties.StorageProperties;
import com.tsingj.sloth.store.utils.CommonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;


import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

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
     * topic-partition log文件内存映射。
     */
    protected final ConcurrentHashMap<String, ConcurrentSkipListMap<Long, LogSegment>> DATA_LOGFILE_MAP = new ConcurrentHashMap<>();


    public LogSegment getLatestLogSegmentFile(String topic, int partition) {
        ConcurrentSkipListMap<Long, LogSegment> logSegmentsSkipListMap = this.DATA_LOGFILE_MAP.get(topic + "_" + partition);
        if (logSegmentsSkipListMap == null || logSegmentsSkipListMap.isEmpty()) {
            return null;
        } else {
            return logSegmentsSkipListMap.lastEntry().getValue();
        }
    }

    public LogSegment newLogSegmentFile(String topic, int partition, long startOffset) {
        //创建文件
        String topicPartitionDirPath = storageProperties.getDataPath() + File.separator + topic + File.separator + partition;
        File dir = new File(topicPartitionDirPath);
        if (!dir.exists()) {
            boolean mkdirs = dir.mkdirs();
            if (mkdirs) {
                logger.info("create datalog dir {} success.", dir);
            } else {
                return null;
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

    public LogSegment findLogSegmentByOffset(String topic, int partition, long offset) {
        ConcurrentSkipListMap<Long, LogSegment> logSegmentsSkipListMap = this.DATA_LOGFILE_MAP.get(topic + "_" + partition);
        if (logSegmentsSkipListMap == null || logSegmentsSkipListMap.isEmpty()) {
            return null;
        }
        Map.Entry<Long, LogSegment> logSegmentEntry = logSegmentsSkipListMap.floorEntry(offset);
        if (logSegmentEntry == null) {
            return null;
        }
        LogSegment logSegment = logSegmentEntry.getValue();
        logger.debug("offset:{} find logSegment startOffset:{}", offset, logSegment.getFileFromOffset());
        return logSegment;
    }

    protected void addLogSegment(String topic, long partition, LogSegment logSegment) {
        ConcurrentSkipListMap<Long, LogSegment> logSegmentsSkipListMap = this.DATA_LOGFILE_MAP.get(topic + "_" + partition);
        if (logSegmentsSkipListMap == null) {
            logSegmentsSkipListMap = new ConcurrentSkipListMap<>();
        }
        logSegmentsSkipListMap.put(logSegment.getFileFromOffset(), logSegment);
        this.DATA_LOGFILE_MAP.put(topic + "_" + partition, logSegmentsSkipListMap);
    }

    public ConcurrentHashMap<String, ConcurrentSkipListMap<Long, LogSegment>> getLogSegmentsMapping() {
        return this.DATA_LOGFILE_MAP;
    }
}
