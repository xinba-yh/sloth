package com.tsingj.sloth.client.consumer;

/**
 * @author yanghao
 * rocketmq kafka区别，同样都支持earliest | latest消费模式， 但rocketmq支持按照指定时间消费(感觉只是锦上添花)。
 */
public enum ConsumeFromWhere {

    /**
     * 当前分组有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费。
     */
    EARLIEST("earliest"),

    /**
     * 当前分组有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的消息。
     */
    LATEST("latest"),
    ;

    private final String type;

    ConsumeFromWhere(String type){
        this.type = type;
    }

    public String getType() {
        return type;
    }


}
