package com.tsingj.sloth.client.springsupport;

import lombok.Data;

import java.util.Map;

/**
 * @author yanghao
 */
@Data
public class ConsumerProperties {

    private String topic;

    private String tag;

    private String consumerMode = "cluster";

    private String groupName;

    private String listener;

    /**
     * 最大消费partition数量
     */
    private Integer maxConsumePartitions = 8;

}
