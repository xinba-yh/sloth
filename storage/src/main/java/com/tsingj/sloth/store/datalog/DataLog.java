package com.tsingj.sloth.store.datalog;

import com.tsingj.sloth.store.*;
import com.tsingj.sloth.store.utils.CompressUtil;
import com.tsingj.sloth.store.utils.CrcUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;


import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yanghao
 */
@Slf4j
@Component
public class DataLog {

    private final DataLogFileSet dataLogFileSet;

    public DataLog(DataLogFileSet dataLogFileSet) {
        this.dataLogFileSet = dataLogFileSet;
    }

    /**
     * 消息存储
     *
     * @param message
     * @return
     */
    public PutMessageResult putMessage(Message message) {
        String topic = message.getTopic();
        int partition = message.getPartition();
        //------------------补充并获取存储信息------------------

        //set 存储时间
        message.setStoreTimestamp(System.currentTimeMillis());
        //set crc
        message.setCrc(CrcUtil.crc32(message.getBody()));
        //set version  1-127
        message.setVersion(CommonConstants.CURRENT_VERSION);

        DataLogFile latestDataLogFile = dataLogFileSet.getLatestDataLogFile(topic, partition);
        //lock start
        long offset;
        if (latestDataLogFile == null || latestDataLogFile.isFull()) {
            offset = latestDataLogFile == null ? 0 : latestDataLogFile.incrementOffsetAndGet();
            latestDataLogFile = dataLogFileSet.getLatestDataLogFile(topic, partition, offset);
            if (latestDataLogFile == null) {
                log.error("create dataLog files error, topic: " + topic + " partition: " + partition);
                return PutMessageResult.builder().status(PutMessageStatus.CREATE_LOG_FILE_FAILED).build();
            }
        } else {
            offset = latestDataLogFile.incrementOffsetAndGet();
        }
        //set 偏移量
        //新创建文件 -> 0 | 当前+1
        //已有未满文件当前+1
        message.setOffset(offset);
        //encode message
        Result<byte[]> encodeResult = StoreEncoder.encode(message);
        if (!encodeResult.success()) {
            return PutMessageResult.builder().status(PutMessageStatus.DATA_ENCODE_FAIL).errorMsg(encodeResult.getMsg()).build();
        }
        byte[] messageBytes = encodeResult.getData();

        //追加文件
        Result appendResult = latestDataLogFile.doAppend(messageBytes, offset, message.getStoreTimestamp());
        if (!appendResult.success()) {
            return PutMessageResult.builder().status(PutMessageStatus.LOG_FILE_APPEND_FAIL).errorMsg(appendResult.getMsg()).build();
        }
        return PutMessageResult.builder().status(PutMessageStatus.OK).offset(offset).build();
    }

    private static int calStoreLength(int bodyLen, int topicLen, int propertiesLen) {
        return DataLogConstants.MessageKeyBytes.STORE +
                DataLogConstants.MessageKeyBytes.STORE_TIMESTAMP +
                DataLogConstants.MessageKeyBytes.VERSION +
                DataLogConstants.MessageKeyBytes.TOPIC +
                topicLen +
                DataLogConstants.MessageKeyBytes.PARTITION +
                DataLogConstants.MessageKeyBytes.PROPERTIES +
                propertiesLen +
                DataLogConstants.MessageKeyBytes.CRC +
                DataLogConstants.MessageKeyBytes.BODY_SIZE +
                bodyLen;
    }

    /**
     * 按照offset获取消息
     *
     * @param topic
     * @param partition
     * @param offset
     * @return
     */
    public GetMessageResult getMessage(String topic, int partition, long offset) {
        List<DataLogFile> dataLogFiles = dataLogFileSet.getDataLogFiles(topic, partition);
        if (dataLogFiles == null) {
            return GetMessageResult.builder().status(GetMessageStatus.PARTITION_NO_MESSAGE).build();
        }

        DataLogFile dataLogFile = dataLogFileSet.findDataLogFileByOffset(dataLogFiles, offset);
        Result<ByteBuffer> getMessageResult = dataLogFile.getMessage(offset);
        if (getMessageResult.failure()) {
            return GetMessageResult.builder().status(GetMessageStatus.OFFSET_FOUND_NULL).build();
        }
        ByteBuffer storeByteBuffer = getMessageResult.getData();
        Result<Message> decodeResult = StoreDecoder.decode(offset, storeByteBuffer);
        if (decodeResult.failure()) {
            return GetMessageResult.builder().status(GetMessageStatus.OFFSET_FOUND_NULL).build();
        }
        return GetMessageResult.builder().status(GetMessageStatus.FOUND).message(decodeResult.getData()).build();
    }


    public static class StoreEncoder {

        public static Result<byte[]> encode(Message message) {
            //---------------基础属性----------------
            byte[] topic = message.getTopic().getBytes(StandardCharsets.UTF_8);
            byte topicLen = (byte) topic.length;
            int partitionId = message.getPartition();
            byte[] properties = messageProperties2String(message.getProperties()).getBytes(StandardCharsets.UTF_8);
            int propertiesLen = properties.length;
            //now used GZIP compress
            //todo get compress way in properties
            Result<byte[]> compressResult = CompressUtil.GZIP.compress(message.getBody());
            if (!compressResult.success()) {
                return compressResult;
            }
            byte[] body = compressResult.getData();
            int bodyLen = body.length;
            //---------------存储附加属性----------------
            long offset = message.getOffset();
            long storeTimestamp = message.getStoreTimestamp();
            int crc = message.getCrc();
            byte version = message.getVersion();
            //this storeLen is after offset
            int storeLen = calStoreLength(bodyLen, topicLen, propertiesLen);

            //------ build append buffer ----
            ByteBuffer storeByteBuffer = ByteBuffer.allocate(DataLogConstants.MessageKeyBytes.OFFSET + storeLen);
            //1、offset - 8
            storeByteBuffer.putLong(offset);
            //2、storeSize - 4
            storeByteBuffer.putInt(storeLen);
            //3、storeTimeStamp - 8
            storeByteBuffer.putLong(storeTimestamp);
            //4、version - 1
            storeByteBuffer.put(version);
            //5、topicLen - 1
            storeByteBuffer.put(topicLen);
            //6、topic - cal
            storeByteBuffer.put(topic);
            //7、partitionId - 4
            storeByteBuffer.putInt(partitionId);
            //8、propertiesLen - 4
            storeByteBuffer.putInt(propertiesLen);
            //9、properties - cal
            storeByteBuffer.put(properties);
            //10、crc - 4
            storeByteBuffer.putInt(crc);
            //11、msgSize - 4
            storeByteBuffer.putInt(bodyLen);
            //12、msgBody - cal
            storeByteBuffer.put(body);
            return Results.success(storeByteBuffer.array());
        }

        private static String messageProperties2String(Map<String, String> properties) {
            StringBuilder sb = new StringBuilder();
            if (properties != null) {
                for (final Map.Entry<String, String> entry : properties.entrySet()) {
                    final String name = entry.getKey();
                    final String value = entry.getValue();

                    sb.append(name);
                    sb.append(CommonConstants.NAME_VALUE_SEPARATOR);
                    sb.append(value);
                    sb.append(CommonConstants.PROPERTY_SEPARATOR);
                }
            }
            return sb.toString();
        }

    }


    public static class StoreDecoder {

        public static Result<Message> decode(long offset, ByteBuffer storeByteBuffer) {
            try {
                Message message = new Message();
                //1、offset - 8
                message.setOffset(offset);
                //2、storeSize - 4
                message.setStoreSize(storeByteBuffer.capacity());
                //3、storeTimeStamp - 8
                long storeTimestamp = storeByteBuffer.getLong();
                message.setStoreTimestamp(storeTimestamp);
                //4、version - 1
                byte version = storeByteBuffer.get();
                message.setVersion(version);
                //5、topicLen - 1
                byte topicLen = storeByteBuffer.get();
                //6、topic - cal
                byte[] topicBytes = new byte[topicLen];
                storeByteBuffer.get(topicBytes);
                String topic = new String(topicBytes, StandardCharsets.UTF_8);
                message.setTopic(topic);
                //7、partitionId - 4
                int partition = storeByteBuffer.getInt();
                message.setPartition(partition);
                //8、propertiesLen - 4
                int propertiesLen = storeByteBuffer.getInt();
                //9、properties - cal
                byte[] propertiesBytes = new byte[propertiesLen];
                storeByteBuffer.get(propertiesBytes);
                Map<String, String> properties = string2messageProperties(new String(propertiesBytes, StandardCharsets.UTF_8));
                message.setProperties(properties);
                //10、crc - 4
                int crc = storeByteBuffer.getInt();
                message.setCrc(crc);
                //11、msgSize - 4
                int msgSize = storeByteBuffer.getInt();
                message.setMsgSize(msgSize);
                //12、msgBody - cal
                byte[] body = new byte[msgSize];
                storeByteBuffer.get(body);
                message.setBody(body);
                return Results.success(message);
            } catch (Throwable e) {
                log.warn("store log message decode error.");
                return Results.failure("store log message decode error.");
            }
        }

        private static Map<String, String> string2messageProperties(final String properties) {
            Map<String, String> map = new HashMap<>(1);
            if (properties != null) {
                String[] items = properties.split(CommonConstants.PROPERTY_SEPARATOR);
                for (String i : items) {
                    String[] nv = i.split(CommonConstants.NAME_VALUE_SEPARATOR);
                    if (2 == nv.length) {
                        map.put(nv[0], nv[1]);
                    }
                }
            }
            return map;
        }

    }


}
