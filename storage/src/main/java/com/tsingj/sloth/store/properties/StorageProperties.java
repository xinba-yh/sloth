package com.tsingj.sloth.store.properties;


import lombok.AccessLevel;
import lombok.Data;
import lombok.experimental.FieldDefaults;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @author yanghao
 */
@Component
@ConfigurationProperties(prefix = "storage")
@Data
@FieldDefaults(level = AccessLevel.PRIVATE)
public class StorageProperties {

    String path = "data";

//    /**
//     * segment文件最大
//     */
//    String segmentMaxFileSize;

}
