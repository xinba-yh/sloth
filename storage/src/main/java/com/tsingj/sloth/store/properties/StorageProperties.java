package com.tsingj.sloth.store.properties;


import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.io.File;

/**
 * @author yanghao
 */
@Component
@ConfigurationProperties(prefix = "storage")
public class StorageProperties {

    /**
     * storage path
     */
    private String dataPath = System.getProperty("user.home") + File.separator + "store";


    /**
     * flush log to disk interval，default 500ms
     */
    private int logFlushInterval = 500;

    /**
     * cleanup expired logs，default default 10分钟
     */
    private int logCleanupInterval = 1000 * 60 * 10;

    /**
     * max message size
     */
    private int messageMaxSize = 1024 * 1024 * 4;

    /**
     * expect segment max fileSize ,  default 1G
     */
    private int segmentMaxFileSize = 1024 * 1024 * 10;

    /**
     * sparse index interval bytes
     */
    private int logIndexIntervalBytes = 1024;


    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public int getMessageMaxSize() {
        return messageMaxSize;
    }

    public void setMessageMaxSize(int messageMaxSize) {
        this.messageMaxSize = messageMaxSize;
    }

    public int getSegmentMaxFileSize() {
        return segmentMaxFileSize;
    }

    public void setSegmentMaxFileSize(int segmentMaxFileSize) {
        this.segmentMaxFileSize = segmentMaxFileSize;
    }

    public int getLogIndexIntervalBytes() {
        return logIndexIntervalBytes;
    }

    public void setLogIndexIntervalBytes(int logIndexIntervalBytes) {
        this.logIndexIntervalBytes = logIndexIntervalBytes;
    }

    public int getLogFlushInterval() {
        return logFlushInterval;
    }

    public void setLogFlushInterval(int logFlushInterval) {
        this.logFlushInterval = logFlushInterval;
    }

    public int getLogCleanupInterval() {
        return logCleanupInterval;
    }

    public void setLogCleanupInterval(int logCleanupInterval) {
        this.logCleanupInterval = logCleanupInterval;
    }
}
