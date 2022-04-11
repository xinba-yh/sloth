package com.tsingj.sloth.store.datalog;


import com.tsingj.sloth.common.SystemClock;
import com.tsingj.sloth.store.constants.LogConstants;
import com.tsingj.sloth.store.datajson.checkpoints.OffsetCheckpointManager;
import com.tsingj.sloth.store.properties.StorageProperties;
import com.tsingj.sloth.store.utils.CommonUtil;
import com.tsingj.sloth.store.utils.StoragePathHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.IntervalTask;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.stereotype.Component;


import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

/**
 * @author yanghao
 * logsegment集合
 * 1、服务启动时完成topic-partition历史数据加载
 * 2、服务启动后按照topic-partition管理logsegment
 */
@EnableScheduling
@Component
public class DataLogManager implements SchedulingConfigurer {

    private static final Logger logger = LoggerFactory.getLogger(DataLogManager.class);

    private final StoragePathHelper storagePathHelper;

    private final StorageProperties storageProperties;

    private final OffsetCheckpointManager offsetCheckpointManager;

    public DataLogManager(StorageProperties storageProperties, StoragePathHelper storagePathHelper, OffsetCheckpointManager offsetCheckpointManager) {
        this.storageProperties = storageProperties;
        this.storagePathHelper = storagePathHelper;
        this.offsetCheckpointManager = offsetCheckpointManager;
    }

    private static final String TOPIC_PARTITION_SEPARATOR = "_";

    /**
     * topic-partition log文件内存映射。
     */
    protected final ConcurrentHashMap<String, ConcurrentSkipListMap<Long, DataLogSegment>> DATA_LOGFILE_MAP = new ConcurrentHashMap<>();


    /**
     * 项目启动时触发
     * -- 加载dataPath目录下所有segment文件和index文件。
     */
    @PostConstruct
    public void load() {
        try {
            logger.info("--------------------prepare load offset checkpoint----------------------");
            offsetCheckpointManager.load();
            logger.info("--------------------prepare load logSegment----------------------");
            //check abnormal stop!
            boolean abnormalStop = CommonUtil.shutdownCleanFileExists(storagePathHelper.getShutdownCleanPath());
            if (abnormalStop) {
                logger.warn("Last time stop is unexpected， maybe abnormal stop!");
            } else {
                boolean newTmpFile = CommonUtil.createShutdownCleanFile(storagePathHelper.getShutdownCleanPath());
                if (!newTmpFile) {
                    return;
                }
            }
            //load logs
            String logDirPath = storagePathHelper.getLogDir();
            File logDir = new File(logDirPath);
            File[] topicFiles = logDir.listFiles();
            if (topicFiles == null || topicFiles.length == 0) {
                logger.info("logDirPath:{} is empty dir, skip initialization.", logDirPath);
                return;
            }
            //initialization segments file memory mapping.
            for (File topicDir : topicFiles) {
                String topic = topicDir.getName();
                File[] partitionDirs = topicDir.listFiles();
                if (partitionDirs == null || partitionDirs.length == 0) {
                    continue;
                }
                for (File partitionDir : partitionDirs) {
                    int partition = Integer.parseInt(partitionDir.getName());
                    File[] segmentFiles = partitionDir.listFiles();
                    if (segmentFiles == null || segmentFiles.length == 0) {
                        continue;
                    }
                    List<File> segmentFileSortedList = Arrays.stream(segmentFiles).sorted(Comparator.comparing(File::getName)).collect(Collectors.toList());
                    for (int i = 0; i < segmentFileSortedList.size(); i++) {
                        File segmentFile = segmentFileSortedList.get(i);
                        String segmentFileName = segmentFile.getName();
                        if (segmentFileName.endsWith(LogConstants.FileSuffix.LOG)) {
                            logger.info("prepare init topic:{} partition:{} segment:{}", topic, partition, segmentFileName);
                            //最后一个文件，为活跃写入文件，异常停止进行checkpoint。
                            boolean checkPoint = abnormalStop && i == segmentFileSortedList.size() - 1;
                            long offsetCheckpoints = 0L;
                            if (checkPoint) {
                                //如果获取不到checkpoints则从头开始。
                                Optional<Long> offsetCheckpointsOptional = offsetCheckpointManager.get(topic, partition);
                                if (offsetCheckpointsOptional.isPresent()) {
                                    offsetCheckpoints = offsetCheckpointsOptional.get();
                                }
                            }
                            /*
                             * 1、初始化LogSegment、OffsetIndex、TimeIndex
                             */
                            String logPath = segmentFile.getAbsolutePath().replace(LogConstants.FileSuffix.LOG, "");
                            long startOffset = CommonUtil.fileName2Offset(segmentFile.getName());
                            DataLogSegment dataLogSegment = new DataLogSegment(logPath, startOffset, storageProperties.getSegmentMaxFileSize(), storageProperties.getLogIndexIntervalBytes());
                            dataLogSegment.load(checkPoint, offsetCheckpoints);
                            this.addLogSegment(topic, partition, dataLogSegment);
                        }
                    }
                }
            }
            logger.info("--------------------load logSegment over----------------------");
        } catch (Throwable e) {
            logger.error("log recovery fail, please check your log!", e);
            throw new DataLogRecoveryException(e);
        }
    }


    @PreDestroy
    public void shutdown() {
        logger.trace("storage prepare destroy.");
        CommonUtil.delShutdownCleanFile(storagePathHelper.getShutdownCleanPath());
        showIndexCacheStats();
        flushDirtyLogs();
        logger.trace("storage destroy done.");
    }


    @Bean
    public TaskScheduler scheduledExecutorService() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(2);
        scheduler.setThreadNamePrefix("scheduled-thread-");
        return scheduler;
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        logger.info("add flushDirtyLogsTask interval {}ms.", storageProperties.getLogFlushInterval());
        IntervalTask flushDirtyLogsTask = new IntervalTask(this::flushDirtyLogs, storageProperties.getLogFlushInterval(), 0);
        taskRegistrar.addFixedDelayTask(flushDirtyLogsTask);

        logger.info("add cleanupLogs interval {}s.", storageProperties.getLogCleanupInterval() / 1000);
        IntervalTask cleanupLogsTask = new IntervalTask(this::cleanupLogs, storageProperties.getLogCleanupInterval(), 0);
        taskRegistrar.addFixedDelayTask(cleanupLogsTask);

        int offsetCheckpointSubmitInterval = storageProperties.getOffsetCheckpointSubmitInterval();
        logger.info("add offsetCheckpoint submit interval {}s.", offsetCheckpointSubmitInterval / 1000);
        IntervalTask offsetCheckpointSubmitTask = new IntervalTask(this::offsetCheckpointSubmit, offsetCheckpointSubmitInterval, offsetCheckpointSubmitInterval);
        taskRegistrar.addFixedDelayTask(offsetCheckpointSubmitTask);
    }


    //----------------------------------------------------------public方法--------------------------------------------------------------------

    public DataLogSegment getLatestLogSegmentFile(String topic, int partition) {
        ConcurrentSkipListMap<Long, DataLogSegment> logSegmentsSkipListMap = this.DATA_LOGFILE_MAP.get(topic + TOPIC_PARTITION_SEPARATOR + partition);
        if (logSegmentsSkipListMap == null || logSegmentsSkipListMap.isEmpty()) {
            return null;
        } else {
            return logSegmentsSkipListMap.lastEntry().getValue();
        }
    }

    public DataLogSegment getFirstLogSegmentFile(String topic, int partition) {
        ConcurrentSkipListMap<Long, DataLogSegment> logSegmentsSkipListMap = this.DATA_LOGFILE_MAP.get(topic + TOPIC_PARTITION_SEPARATOR + partition);
        if (logSegmentsSkipListMap == null || logSegmentsSkipListMap.isEmpty()) {
            return null;
        } else {
            return logSegmentsSkipListMap.firstEntry().getValue();
        }
    }

    public DataLogSegment newLogSegmentFile(String topic, int partition, long startOffset) {
        //创建文件
        String topicPartitionDirPath = storagePathHelper.getLogDir() + File.separator + topic + File.separator + partition;
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
        DataLogSegment newDataLogSegment;
        try {
            newDataLogSegment = new DataLogSegment(logPath, startOffset, storageProperties.getSegmentMaxFileSize(), storageProperties.getLogIndexIntervalBytes());
        } catch (FileNotFoundException e) {
            return null;
        }
        this.addLogSegment(topic, partition, newDataLogSegment);
        return newDataLogSegment;
    }

    public DataLogSegment findLogSegmentByOffset(String topic, int partition, long offset) {
        ConcurrentSkipListMap<Long, DataLogSegment> logSegmentsSkipListMap = this.DATA_LOGFILE_MAP.get(topic + TOPIC_PARTITION_SEPARATOR + partition);
        if (logSegmentsSkipListMap == null || logSegmentsSkipListMap.isEmpty()) {
            return null;
        }
        Map.Entry<Long, DataLogSegment> logSegmentEntry = logSegmentsSkipListMap.floorEntry(offset);
        if (logSegmentEntry == null) {
            return null;
        }
        DataLogSegment dataLogSegment = logSegmentEntry.getValue();
        logger.debug("offset:{} find logSegment startOffset:{}", offset, dataLogSegment.getFileFromOffset());
        return dataLogSegment;
    }

    public ConcurrentHashMap<String, ConcurrentSkipListMap<Long, DataLogSegment>> getLogSegmentsMapping() {
        return this.DATA_LOGFILE_MAP;
    }


    //----------------------------------------------------------private方法--------------------------------------------------------------------

    private void addLogSegment(String topic, long partition, DataLogSegment dataLogSegment) {
        ConcurrentSkipListMap<Long, DataLogSegment> logSegmentsSkipListMap = this.DATA_LOGFILE_MAP.get(topic + TOPIC_PARTITION_SEPARATOR + partition);
        if (logSegmentsSkipListMap == null) {
            logSegmentsSkipListMap = new ConcurrentSkipListMap<>();
        }
        logSegmentsSkipListMap.put(dataLogSegment.getFileFromOffset(), dataLogSegment);
        this.DATA_LOGFILE_MAP.put(topic + TOPIC_PARTITION_SEPARATOR + partition, logSegmentsSkipListMap);
    }

    /**
     * 输出indexCacheStats
     */
    private void showIndexCacheStats() {
        ConcurrentHashMap<String, ConcurrentSkipListMap<Long, DataLogSegment>> logSegmentsMapping = this.getLogSegmentsMapping();
        if (logSegmentsMapping.isEmpty()) {
            return;
        }
        for (Map.Entry<String, ConcurrentSkipListMap<Long, DataLogSegment>> entry : logSegmentsMapping.entrySet()) {
            ConcurrentSkipListMap<Long, DataLogSegment> logSegmentSkipListMap = entry.getValue();
            if (logSegmentSkipListMap.isEmpty()) {
                continue;
            }
            Collection<DataLogSegment> dataLogSegments = logSegmentSkipListMap.values();
            for (DataLogSegment dataLogSegment : dataLogSegments) {
                dataLogSegment.getOffsetIndex().showIndexCacheStats();
            }
        }
    }

    /**
     * 定时刷新日志至磁盘
     */
    private void flushDirtyLogs() {
        logger.debug("prepare flush dirtyLogs.");
        ConcurrentHashMap<String, ConcurrentSkipListMap<Long, DataLogSegment>> logSegmentsMapping = this.getLogSegmentsMapping();
        if (logSegmentsMapping.isEmpty()) {
            return;
        }
        for (Map.Entry<String, ConcurrentSkipListMap<Long, DataLogSegment>> entry : logSegmentsMapping.entrySet()) {
            ConcurrentSkipListMap<Long, DataLogSegment> logSegmentSkipListMap = entry.getValue();
            if (logSegmentSkipListMap.isEmpty()) {
                continue;
            }
            Collection<DataLogSegment> dataLogSegments = logSegmentSkipListMap.values();
            for (DataLogSegment dataLogSegment : dataLogSegments) {
                dataLogSegment.flush();
                dataLogSegment.getOffsetIndex().flush();
                dataLogSegment.getOffsetIndex().freeNoWarmIndexCache();
                dataLogSegment.getTimeIndex().flush();
            }
        }
        logger.debug("flush dirtyLogs done.");
    }

    /**
     *
     */
    private void offsetCheckpointSubmit() {
        logger.debug("prepare offsetCheckpoint submit.");
        ConcurrentHashMap<String, ConcurrentSkipListMap<Long, DataLogSegment>> logSegmentsMapping = this.getLogSegmentsMapping();
        if (logSegmentsMapping.isEmpty()) {
            return;
        }
        for (Map.Entry<String, ConcurrentSkipListMap<Long, DataLogSegment>> entry : logSegmentsMapping.entrySet()) {
            String topicPartition = entry.getKey();
            String[] topicPartitionArr = topicPartition.split(TOPIC_PARTITION_SEPARATOR);
            String topic = topicPartitionArr[0];
            Integer partition = Integer.parseInt(topicPartitionArr[1]);
            ConcurrentSkipListMap<Long, DataLogSegment> logSegmentSkipListMap = entry.getValue();
            if (logSegmentSkipListMap.isEmpty()) {
                continue;
            }
            DataLogSegment dataLogSegment = logSegmentSkipListMap.lastEntry().getValue();
            logger.debug("Topic:{} partition:{} submit flushedOffset:{}.", topic, partition, dataLogSegment.getFlushedOffset());
            offsetCheckpointManager.submit(topic, partition, dataLogSegment.getFlushedOffset());
        }
        offsetCheckpointManager.persist();
    }

    /**
     * 过期log segment清理
     */
    private synchronized void cleanupLogs() {
        logger.info("cleanupLogs");
        //日志保存时间
        int logRetentionHours = storageProperties.getLogRetentionHours();

        //todo 日志保存大小
        long logRetentionBytes = storageProperties.getLogRetentionBytes();

        //符合其一则清理，按照segment为最小粒度。
        //1、基于时间策略清理
        //注意：如果一个segment开始时间符合，但结束时间不符合，不清理。
        ConcurrentHashMap<String, ConcurrentSkipListMap<Long, DataLogSegment>> logSegmentsMapping = this.getLogSegmentsMapping();
        if (logSegmentsMapping.isEmpty()) {
            return;
        }
        for (Map.Entry<String, ConcurrentSkipListMap<Long, DataLogSegment>> entry : logSegmentsMapping.entrySet()) {
            String topicPartition = entry.getKey();
            ConcurrentSkipListMap<Long, DataLogSegment> logSegmentSkipListMap = entry.getValue();
            if (logSegmentSkipListMap.isEmpty()) {
                continue;
            }
            for (Map.Entry<Long, DataLogSegment> logSegmentSkipListMapEntry : logSegmentSkipListMap.entrySet()) {
                Long dataLogSegmentKey = logSegmentSkipListMapEntry.getKey();
                DataLogSegment dataLogSegment = logSegmentSkipListMapEntry.getValue();
                long largestTimestamp = dataLogSegment.getLargestTimestamp();
                if (largestTimestamp == 0) {
                    logger.warn("Topic-partition:{} logSegment:{} got largestTimestamp eq 0!", topicPartition, dataLogSegmentKey);
                    continue;
                }
                if ((SystemClock.now() - CommonUtil.hourToMills(logRetentionHours)) > largestTimestamp) {
                    //删除物理Log文件以及相关索引文件
                    dataLogSegment.delete();
                    logSegmentSkipListMap.remove(dataLogSegmentKey);
                    logger.info("Deleted expire Log segment {} {}.", topicPartition, dataLogSegmentKey);
                }
            }
        }

        //2、基于保存大小清理
        //注意：如果总大小 占用超过80% 则清理最早所有topic-partition 首个segment文件。

    }


}

