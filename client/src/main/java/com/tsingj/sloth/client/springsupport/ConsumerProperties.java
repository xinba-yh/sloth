package com.tsingj.sloth.client.springsupport;

import com.tsingj.sloth.client.consumer.ConsumeFromWhere;
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

    private String consumeFromWhere = ConsumeFromWhere.EARLIEST;

    /**
     * 最大消费partition数量
     */
    private Integer maxConsumePartitions = 8;

    /**
     * partition消费时，无消息可以消费状态拉取间隔
     * if fetch interval has new message , broker will notify weekUp message
     */
    private Integer consumeWhenNoMessageInterval = 1000;

    /**
     * partition消费时，默认错误休眠时间，default 200ms
     * client-server通讯失败
     */
    private Integer consumeWhenErrorSleepTimeDefault = 200;

    /**
     * partition消费时，最大错误休眠时间，default 5000ms
     * client-server通讯失败
     */
    private Integer consumeWhenErrorSleepTimeMax = 5000;

    /**
     * partition消费时，默认listener执行错误休眠时间，default 1S
     * 实际休眠时长为：consumeWhenListenerErrorSleepTimeDefault * 失败次数
     */
    private Integer consumeWhenListenerErrorSleepTimeDefault = 1000;

    /**
     * 消息失败最大次数
     */
    private Integer consumeWhenListenerErrorMaxTimes = 16;

}
