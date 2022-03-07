package com.tsingj.sloth.store.utils;

import com.tsingj.sloth.store.properties.StorageProperties;
import org.springframework.stereotype.Component;

import java.io.File;

/**
 * @author yanghao
 */
@Component
public class StoragePathHelper {

    private final StorageProperties storageProperties;

    private final String metadataDir = "metadata";

    private final String logDir = "logs";

    public StoragePathHelper(StorageProperties storageProperties) {
        this.storageProperties = storageProperties;
    }



    public String getConfigDir() {
        return storageProperties.getDataPath() + File.separator + metadataDir;
    }

    public String getTopicPath() {
        return getConfigDir() + File.separator + "topics.json";
    }

    public String getLogDir() {
        return storageProperties.getDataPath() + File.separator + logDir;
    }

}
