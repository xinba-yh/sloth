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
//    String dataPath = System.getProperty("user.home") + File.separator + "data";

    private String dataPath = "/Users/yanghao" + File.separator + "data";


    /**
     * flush data to disk interval
     */
    private int dataFlushInterval = 500;

    /**
     * max message size
     */
    private int messageMaxSize = 1024 * 1024 * 4;

    /**
     * expect segment max fileSize ,  default 1G
     */
    private int segmentMaxFileSize = 1024 * 1024 * 256;

    /**
     * sparse index interval bytes
     */
    private int logIndexIntervalBytes = 1024 * 1;


    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public int getDataFlushInterval() {
        return dataFlushInterval;
    }

    public void setDataFlushInterval(int dataFlushInterval) {
        this.dataFlushInterval = dataFlushInterval;
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
}
