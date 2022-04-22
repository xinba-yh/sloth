package com.tsingj.sloth.common;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;
import java.util.Map;

public class SerializationTest {

    public static void main(String[] args) {
        Message message = new Message();
        message.setTopic("aaa");
        message.setPartition(1);
        byte[] bytes = ProtoStuffSerializer.serialize(message);
        Message message1 = ProtoStuffSerializer.deserialize(bytes, Message.class);
        System.out.println(JSON.toJSONString(message1));
    }

    public static class Message implements Serializable {

        //--------------------基本属性--------------------

        private String topic;

        private int partition;

//    private String messageId;

        private Map<String, String> properties;

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

        public void setMsgSize(int msgSize) {
            this.msgSize = msgSize;
        }

        public int getCrc() {
            return crc;
        }

        public void setCrc(int crc) {
            this.crc = crc;
        }

//    public String getMessageId() {
//        return messageId;
//    }
//
//    public void setMessageId(String messageId) {
//        this.messageId = messageId;
//    }

        @Override
        public String toString() {
            return "Message{" +
                    "topic='" + topic + '\'' +
                    ", partition=" + partition +
//                ", messageId='" + messageId + '\'' +
                    ", properties=" + properties +
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
}


