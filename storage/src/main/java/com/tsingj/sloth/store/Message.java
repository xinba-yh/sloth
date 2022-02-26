package com.tsingj.sloth.store;

import lombok.*;
import lombok.experimental.FieldDefaults;

import java.io.Serializable;
import java.util.Map;

/**
 * @author yanghao
 * message基本属性和存储属性放在一个类里，不再做额外的copy。
 */
@Data
@Builder
@FieldDefaults(level = AccessLevel.PRIVATE)
@NoArgsConstructor
@AllArgsConstructor
public class Message implements Serializable {

    //--------------------基本属性--------------------

    String topic;

    Map<String, String> properties;

    int partition;

    byte[] body;


    //--------------------存储属性--------------------

    long offset;

    /**
     * 存储大小
     */
    int storeSize;

    /**
     * 存储时间
     */
    long storeTimestamp;

    /**
     * 版本号
     */
    byte version;

    /**
     * message大小
     */
    int msgSize;

    /**
     * body crc校验码
     */
    int crc;


}
