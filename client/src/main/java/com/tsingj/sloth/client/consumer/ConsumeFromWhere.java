package com.tsingj.sloth.client.consumer;

/**
 * @author yanghao
 * rocketmq kafka区别，同样都支持earliest | latest消费模式， 但rocketmq支持按照指定时间消费。
 * earliest:当前分组有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费。
 * latest:当前分组有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的消息。
 * todo 暂时支持first和last，按照时间消费只需要按照timestamp找到对应的offset即可。
 */
public class ConsumeFromWhere {

    public final static String EARLIEST = "earliest";

    public final static String LATEST = "latest";

}
