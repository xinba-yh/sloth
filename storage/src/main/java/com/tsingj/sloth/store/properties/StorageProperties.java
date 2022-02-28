package com.tsingj.sloth.store.properties;


import lombok.AccessLevel;
import lombok.Data;
import lombok.experimental.FieldDefaults;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.io.File;

/**
 * @author yanghao
 */
@Component
@ConfigurationProperties(prefix = "storage")
@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
public class StorageProperties {

    /**
     * storage path
     */
//    String dataPath = System.getProperty("user.home") + File.separator + "data";

    String dataPath = "/Users/yanghao" + File.separator + "data";


    /**
     * flush data to disk interval
     */
    int dataFlushInterval = 500;

    /**
     * max message size
     */
    int messageMaxSize = 1024 * 1024 * 4;

    /**
     * expect segment max fileSize ,  default 1G
     */
    int segmentMaxFileSize = 1024 * 1024 * 10;

    /**
     * sparse index interval bytes
     */
    int logIndexIntervalBytes = 1024 * 4;

}
