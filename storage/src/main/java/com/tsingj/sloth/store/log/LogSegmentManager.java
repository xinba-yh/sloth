package com.tsingj.sloth.store.log;


import com.tsingj.sloth.store.DataRecovery;
import com.tsingj.sloth.store.constants.LogConstants;
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
public class LogSegmentManager implements SchedulingConfigurer, DataRecovery {

    private static final Logger logger = LoggerFactory.getLogger(LogSegmentManager.class);

    private final StoragePathHelper storagePathHelper;

    private final StorageProperties storageProperties;

    public LogSegmentManager(StorageProperties storageProperties, StoragePathHelper storagePathHelper) {
        this.storageProperties = storageProperties;
        this.storagePathHelper = storagePathHelper;
    }

    /**
     * topic-partition log文件内存映射。
     */
    protected final ConcurrentHashMap<String, ConcurrentSkipListMap<Long, LogSegment>> DATA_LOGFILE_MAP = new ConcurrentHashMap<>();


    /**
     * 项目启动时触发
     * -- 加载dataPath目录下所有segment文件和index文件。
     */
    @PostConstruct
    @Override
    public void load() {
        logger.info("--------------------prepare load logSegment----------------------");
        String logDirPath = storagePathHelper.getLogDir();
        try {
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
                    long partition = Integer.parseInt(partitionDir.getName());
                    File[] segmentFiles = partitionDir.listFiles();
                    if (segmentFiles == null || segmentFiles.length == 0) {
                        continue;
                    }
                    List<File> segmentFileSortedList = Arrays.stream(segmentFiles).sorted(Comparator.comparing(File::getName)).collect(Collectors.toList());
                    for (File segmentFile : segmentFileSortedList) {
                        String segmentFileName = segmentFile.getName();
                        if (segmentFileName.endsWith(LogConstants.FileSuffix.LOG)) {
                            logger.info("prepare init topic:{} partition:{} segment:{}", topic, partition, segmentFileName);
                            /*
                             * 1、初始化LogSegment、OffsetIndex、TimeIndex
                             */
                            String logPath = segmentFile.getAbsolutePath().replace(LogConstants.FileSuffix.LOG, "");
                            long startOffset = CommonUtil.fileName2Offset(segmentFile.getName());
                            LogSegment logSegment = new LogSegment(logPath, startOffset, storageProperties.getSegmentMaxFileSize(), storageProperties.getLogIndexIntervalBytes());
                            logSegment.load();
                            this.addLogSegment(topic, partition, logSegment);
                        }
                    }
                }
            }
            logger.info("--------------------load logSegment over----------------------");
        } catch (Throwable e) {
            logger.error("log recovery fail, please check your log!", e);
            throw new LogRecoveryException(e);
        }
    }


    @PreDestroy
    public void shutdown() {
        logger.trace("storage prepare destroy.");
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
    }

    //----------------------------------------------------------public方法--------------------------------------------------------------------

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

    public ConcurrentHashMap<String, ConcurrentSkipListMap<Long, LogSegment>> getLogSegmentsMapping() {
        return this.DATA_LOGFILE_MAP;
    }


    //----------------------------------------------------------private方法--------------------------------------------------------------------

    private void addLogSegment(String topic, long partition, LogSegment logSegment) {
        ConcurrentSkipListMap<Long, LogSegment> logSegmentsSkipListMap = this.DATA_LOGFILE_MAP.get(topic + "_" + partition);
        if (logSegmentsSkipListMap == null) {
            logSegmentsSkipListMap = new ConcurrentSkipListMap<>();
        }
        logSegmentsSkipListMap.put(logSegment.getFileFromOffset(), logSegment);
        this.DATA_LOGFILE_MAP.put(topic + "_" + partition, logSegmentsSkipListMap);
    }

    /**
     * 输出indexCacheStats
     */
    private void showIndexCacheStats() {
        ConcurrentHashMap<String, ConcurrentSkipListMap<Long, LogSegment>> logSegmentsMapping = this.getLogSegmentsMapping();
        if (logSegmentsMapping.isEmpty()) {
            return;
        }
        for (Map.Entry<String, ConcurrentSkipListMap<Long, LogSegment>> entry : logSegmentsMapping.entrySet()) {
            ConcurrentSkipListMap<Long, LogSegment> logSegmentSkipListMap = entry.getValue();
            if (logSegmentSkipListMap.isEmpty()) {
                continue;
            }
            Collection<LogSegment> logSegments = logSegmentSkipListMap.values();
            for (LogSegment logSegment : logSegments) {
                logSegment.getOffsetIndex().showIndexCacheStats();
            }
        }
    }

    /**
     * 定时刷新日志至磁盘
     */
    private void flushDirtyLogs() {
        logger.debug("prepare flush dirtyLogs.");
        ConcurrentHashMap<String, ConcurrentSkipListMap<Long, LogSegment>> logSegmentsMapping = this.getLogSegmentsMapping();
        if (logSegmentsMapping.isEmpty()) {
            return;
        }
        for (Map.Entry<String, ConcurrentSkipListMap<Long, LogSegment>> entry : logSegmentsMapping.entrySet()) {
            ConcurrentSkipListMap<Long, LogSegment> logSegmentSkipListMap = entry.getValue();
            if (logSegmentSkipListMap.isEmpty()) {
                continue;
            }
            Collection<LogSegment> logSegments = logSegmentSkipListMap.values();
            for (LogSegment logSegment : logSegments) {
                logSegment.flush();
                logSegment.getOffsetIndex().flush();
                logSegment.getOffsetIndex().freeNoWarmIndexCache();
                logSegment.getTimeIndex().flush();
            }
        }
        logger.debug("flush dirtyLogs done.");
    }


    /**
     * 历史segment清理
     */
    private void cleanupLogs() {
        logger.info("cleanupLogs");
    }


}