package com.tsingj.sloth.store.log;

import com.tsingj.sloth.store.constants.LogConstants;
import com.tsingj.sloth.store.properties.StorageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.IntervalTask;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author yanghao
 */
@EnableScheduling
@Component
public class LogManager implements SchedulingConfigurer {

    private static final Logger logger = LoggerFactory.getLogger(LogManager.class);

    private final LogSegmentSet logSegmentSet;

    private final StorageProperties storageProperties;

    public LogManager(LogSegmentSet logSegmentSet, StorageProperties storageProperties) {
        this.logSegmentSet = logSegmentSet;
        this.storageProperties = storageProperties;
    }

    /**
     * 项目启动时触发
     * -- 加载dataPath目录下所有segment文件和index文件。
     */
    @PostConstruct
    public void loadLogs() {
        String dataPath = storageProperties.getDataPath();
        try {
            File dataDir = new File(dataPath);
            File[] topicFiles = dataDir.listFiles();
            if (topicFiles == null || topicFiles.length == 0) {
                logger.info("dataPath:{} is empty dir, skip initialization.", dataPath);
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
                            LogSegment logSegment = LogSegment.loadLogs(segmentFile, storageProperties.getSegmentMaxFileSize(), storageProperties.getLogIndexIntervalBytes());
                            logSegmentSet.addLogSegment(topic, partition, logSegment);
                        }
                    }
                }
            }
            logger.info("-------------------------------------------------load segments over----------------------------------------------------------------");
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }


    @PreDestroy
    public void shutdown() {
        logger.trace("storage prepare destroy.");
        showIndexCacheStats();
        flushDirtyLogs();
        logger.trace("storage destroy done.");
    }

    /**
     * 输出indexCacheStats
     */
    private void showIndexCacheStats() {
        Map<String, List<LogSegment>> logSegmentsMapping = logSegmentSet.getLogSegmentsMapping();
        if (logSegmentsMapping.size() == 0) {
            return;
        }
        for (Map.Entry<String, List<LogSegment>> entry : logSegmentsMapping.entrySet()) {
            List<LogSegment> logSegments = entry.getValue();
            for (LogSegment logSegment : logSegments) {
                logSegment.getOffsetIndex().showIndexCacheStats();
            }
        }
    }


    /**
     * 定时刷新日志至磁盘
     */
    public void flushDirtyLogs() {
        logger.debug("prepare flush dirtyLogs.");
        Map<String, List<LogSegment>> logSegmentsMapping = logSegmentSet.getLogSegmentsMapping();
        if (logSegmentsMapping.size() == 0) {
            return;
        }
        for (Map.Entry<String, List<LogSegment>> entry : logSegmentsMapping.entrySet()) {
            List<LogSegment> logSegments = entry.getValue();
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
    public void cleanupLogs() {
        logger.info("cleanupLogs");
    }


    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        logger.info("add flushDirtyLogsTask interval {}ms.", storageProperties.getLogFlushInterval());
        IntervalTask flushDirtyLogsTask = new IntervalTask(this::flushDirtyLogs, storageProperties.getLogFlushInterval(), 0);
        taskRegistrar.addFixedDelayTask(flushDirtyLogsTask);

        logger.info("add cleanupLogs interval {}ms.", storageProperties.getLogCleanupInterval());
        IntervalTask cleanupLogsTask = new IntervalTask(this::cleanupLogs, storageProperties.getLogCleanupInterval(), 0);
        taskRegistrar.addFixedDelayTask(cleanupLogsTask);
    }
}
