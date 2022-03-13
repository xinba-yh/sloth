package com.tsingj.sloth.rpcmodel.protocol;

import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;

import java.util.Map;

/**
 * @author yanghao
 */
public class Message {

    @Protobuf
    private String topic;

    @Protobuf
    private int partition;

    @Protobuf
    private Map<String, String> properties;

    @Protobuf
    private byte[] body; //业务侧消息体

    @Protobuf
    private boolean ack; //是否需要ack 默认：true

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public boolean isAck() {
        return ack;
    }

    public void setAck(boolean ack) {
        this.ack = ack;
    }
}
