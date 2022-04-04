package com.tsingj.sloth.client.consumer;

/**
 * @author yanghao
 */
public enum ConsumeMode {

    /**
     * 同一个消费者组，每个消息只会有一个消费者可以消费。
     */
    CLUSTER("cluster"),

    /**
     * 同一个消费者组，全部的消费者都都可以消费全部的消息。
     */
//    BROADCAST("broadcast"),
    ;

    private final String type;

    ConsumeMode(String type){
        this.type = type;
    }

    public String getType() {
        return type;
    }


}
