package com.tsingj.sloth.store.log;

import com.tsingj.sloth.store.*;
import com.tsingj.sloth.store.utils.CompressUtil;
import com.tsingj.sloth.store.utils.CrcUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;


import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yanghao
 */
@Component
public class Log {

    private static final Logger logger = LoggerFactory.getLogger(Log.class);

    private final LogSegmentSet logSegmentSet;

    public Log(LogSegmentSet logSegmentSet) {
        this.logSegmentSet = logSegmentSet;
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

        LogSegment latestLogSegment = logSegmentSet.getLatestDataLogFile(topic, partition);
        //lock start
        long offset;
        if (latestLogSegment == null || latestLogSegment.isFull()) {
            offset = latestLogSegment == null ? 0 : latestLogSegment.incrementOffsetAndGet();
            latestLogSegment = logSegmentSet.getLatestDataLogFile(topic, partition, offset);
            if (latestLogSegment == null) {
                logger.error("create dataLog files error, topic: " + topic + " partition: " + partition);
                return PutMessageResult.builder().status(PutMessageStatus.CREATE_LOG_FILE_FAILED).build();
            }
        } else {
            offset = latestLogSegment.incrementOffsetAndGet();
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
        Result appendResult = latestLogSegment.doAppend(messageBytes, offset, message.getStoreTimestamp());
        if (!appendResult.success()) {
            return PutMessageResult.builder().status(PutMessageStatus.LOG_FILE_APPEND_FAIL).errorMsg(appendResult.getMsg()).build();
        }
        return PutMessageResult.builder().status(PutMessageStatus.OK).offset(offset).build();
    }

    private static int calStoreLength(int bodyLen, int topicLen, int propertiesLen) {
        return  DataLogConstants.MessageKeyBytes.STORE_TIMESTAMP +
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
        StopWatch sw = new StopWatch();
//        sw.start("logSegmentSet.getLogSegments");
        List<LogSegment> logSegments = logSegmentSet.getLogSegments(topic, partition);
        if (logSegments == null) {
            return GetMessageResult.builder().status(GetMessageStatus.PARTITION_NO_MESSAGE).build();
        }
//        sw.stop();
//        sw.start("logSegmentSet.findLogSegmentByOffset");
        LogSegment logSegment = logSegmentSet.findLogSegmentByOffset(logSegments, offset);
//        sw.stop();
        sw.start("logSegment.getMessage");
        Result<ByteBuffer> getMessageResult = logSegment.getMessage(offset);
        sw.stop();
        if (getMessageResult.failure()) {
            return GetMessageResult.builder().status(GetMessageStatus.OFFSET_FOUND_NULL).build();
        }
        sw.start("logSegment.StoreDecoder.decode");
        ByteBuffer storeByteBuffer = getMessageResult.getData();
        Result<Message> decodeResult = StoreDecoder.decode(offset, storeByteBuffer);
        if (decodeResult.failure()) {
            return GetMessageResult.builder().status(GetMessageStatus.MESSAGE_DECODE_FAIL).build();
        }
        sw.stop();
        logger.debug(sw.prettyPrint() + "\n total mills:" + sw.getTotalTimeMillis());
        logger.debug("-------------------------------------------------------------");
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
            ByteBuffer storeByteBuffer = ByteBuffer.allocate(DataLogConstants.MessageKeyBytes.LOG_OVERHEAD + storeLen);
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
                //position reset
                storeByteBuffer.flip();
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
                logger.error("store log message decode error.", e);
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
