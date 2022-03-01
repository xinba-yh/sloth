package com.tsingj.sloth.store;


import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

/**
 * @author yanghao
 * message基本属性和存储属性放在一个类里，不再做额外的copy。
 */
public class Message implements Serializable {

    //--------------------基本属性--------------------

    private String topic;

    private Map<String, String> properties;

    private int partition;

    private byte[] body;


    //--------------------存储属性--------------------

    private long offset;

    /**
     * 存储大小
     */
    private int storeSize;

    /**
     * 存储时间
     */
    private long storeTimestamp;

    /**
     * 版本号
     */
    private byte version;

    /**
     * message大小
     */
    private int msgSize;

    /**
     * body crc校验码
     */
    private int crc;


    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getStoreSize() {
        return storeSize;
    }

    public void setStoreSize(int storeSize) {
        this.storeSize = storeSize;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public byte getVersion() {
        return version;
    }

    public void setVersion(byte version) {
        this.version = version;
    }

    public int getMsgSize() {
        return msgSize;
    }

    public void setMsgSize(int msgSize) {
        this.msgSize = msgSize;
    }

    public int getCrc() {
        return crc;
    }

    public void setCrc(int crc) {
        this.crc = crc;
    }


    @Override
    public String toString() {
        return "Message{" +
                "topic='" + topic + '\'' +
                ", properties=" + properties +
                ", partition=" + partition +
                ", body=" + body.length +
                ", offset=" + offset +
                ", storeSize=" + storeSize +
                ", storeTimestamp=" + storeTimestamp +
                ", version=" + version +
                ", msgSize=" + msgSize +
                ", crc=" + crc +
                '}';
    }
}
