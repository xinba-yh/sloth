package com.tsingj.sloth.store.utils;

import com.tsingj.sloth.store.properties.StorageProperties;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * @author yanghao
 */
@Component
public class StoragePathHelper {

    private final StorageProperties storageProperties;

    private final String metadataDir = "metadata";

    private final String logDir = "logs";

    private final String raftLogDir = "raft";

    public StoragePathHelper(StorageProperties storageProperties) {
        this.storageProperties = storageProperties;
    }


    public String getLogDir() {
        return storageProperties.getDataPath() + File.separator + logDir;
    }

    public String getRaftLogDir() {
        return storageProperties.getDataPath() + File.separator + raftLogDir;
    }

    public String getDataJsonDir() {
        return storageProperties.getDataPath() + File.separator + metadataDir;
    }

    public String getTopicPath() {
        return getDataJsonDir() + File.separator + "topics.json";
    }

    public String getConsumerOffsetsPath() {
        return getDataJsonDir() + File.separator + "consumerOffsets.json";
    }

    public String getRecoveryPointOffsetCheckpointPath() {
        return getDataJsonDir() + File.separator + "recoveryPointOffsetCheckpoint.json";
    }

    /**
     * 判断是否为异常停止的文件
     *
     * @return
     */
    public String getShutdownCleanPath() {
        return storageProperties.getDataPath() + File.separator + ".shutdownClean";
    }

}
