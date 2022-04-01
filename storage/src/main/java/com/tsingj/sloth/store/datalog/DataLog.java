package com.tsingj.sloth.store.datalog;

import com.tsingj.sloth.common.SystemClock;
import com.tsingj.sloth.common.result.Result;
import com.tsingj.sloth.common.result.Results;
import com.tsingj.sloth.store.constants.CommonConstants;
import com.tsingj.sloth.store.constants.LogConstants;
import com.tsingj.sloth.store.datalog.lock.LogLock;
import com.tsingj.sloth.store.datalog.lock.LogLockFactory;
import com.tsingj.sloth.store.pojo.*;
import com.tsingj.sloth.store.utils.CommonUtil;
import com.tsingj.sloth.store.utils.CompressUtil;
import com.tsingj.sloth.store.utils.CrcUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;


import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * @author yanghao
 * 提供消息存储和获取能力
 */
@Component
public class DataLog {

    private static final Logger logger = LoggerFactory.getLogger(DataLog.class);

    private final DataLogSegmentManager dataLogSegmentManager;

    public DataLog(DataLogSegmentManager dataLogSegmentManager) {
        this.dataLogSegmentManager = dataLogSegmentManager;
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
        message.setStoreTimestamp(SystemClock.now());
        //set crc
        message.setCrc(CrcUtil.crc32(message.getBody()));
        //set version  1-127
        message.setVersion(CommonConstants.CURRENT_VERSION);

        //get log lock, 每个topic、partition同时仅可以有一个线程进行写入，并发写入的提速在partition层。
        //kafka used synchronized , rocketmq used spinLock(AtomicBoolean) , 实现该部分流程不同，需要采用topic-partition分段锁，使用ReentrantLock.
        LogLock lock = LogLockFactory.getReentrantLock(topic, partition);

        long offset;
        try {
            //lock start
            lock.lock();
            //find latest logSegmentFile
            DataLogSegment latestDataLogSegment = dataLogSegmentManager.getLatestLogSegmentFile(topic, partition);
            //1、not found create
            //2、full rolling logSegmentFile with index
            if (latestDataLogSegment == null || latestDataLogSegment.isFull()) {
                offset = latestDataLogSegment == null ? 0 : latestDataLogSegment.incrementOffsetAndGet();
                latestDataLogSegment = dataLogSegmentManager.newLogSegmentFile(topic, partition, offset);
                if (latestDataLogSegment == null) {
                    logger.error("create dataLog files error, topic: " + topic + " partition: " + partition);
                    return new PutMessageResult(PutMessageStatus.CREATE_LOG_FILE_FAILED);
                }
            } else {
                //3、found and not full,increment offset
                offset = latestDataLogSegment.incrementOffsetAndGet();
            }
            //check offset
            if (offset < latestDataLogSegment.getFileFromOffset()) {
                return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, "offset was reset!");
            }
            //set 偏移量
            //新创建文件 -> 0 | 当前+1
            //已有未满文件当前+1
            message.setOffset(offset);
            //encode message
            Result<ByteBuffer> encodeResult = StoreEncoder.encode(message);
            if (!encodeResult.success()) {
                return new PutMessageResult(PutMessageStatus.DATA_ENCODE_FAIL, encodeResult.getMsg());
            }
            ByteBuffer messageBytes = encodeResult.getData();

            //追加文件
            Result appendResult = latestDataLogSegment.doAppend(messageBytes, offset, message.getStoreTimestamp());
            if (!appendResult.success()) {
                return new PutMessageResult(PutMessageStatus.LOG_FILE_APPEND_FAIL, appendResult.getMsg());
            }
        } catch (Throwable e) {
            logger.error("put message error!", e);
            return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, e.getMessage());
        } finally {
            lock.unlock();
        }
        return new PutMessageResult(PutMessageStatus.OK,topic,partition, offset);
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
        DataLogSegment dataLogSegment = dataLogSegmentManager.findLogSegmentByOffset(topic, partition, offset);
        if (dataLogSegment == null) {
            return new GetMessageResult(GetMessageStatus.LOG_SEGMENT_NOT_FOUND);
        }
        try {
            Result<ByteBuffer> getMessageResult = dataLogSegment.getMessage(offset);
            if (getMessageResult.failure()) {
                return new GetMessageResult(GetMessageStatus.OFFSET_NOT_FOUND, getMessageResult.getMsg());
            }
            ByteBuffer storeByteBuffer = getMessageResult.getData();
            Result<Message> decodeResult = StoreDecoder.decode(offset, storeByteBuffer);
            if (decodeResult.failure()) {
                return new GetMessageResult(GetMessageStatus.MESSAGE_DECODE_FAIL, decodeResult.getMsg());
            }
            return new GetMessageResult(GetMessageStatus.FOUND, decodeResult.getData());
        } catch (Throwable e) {
            return new GetMessageResult(GetMessageStatus.UNKNOWN_ERROR, e.getMessage());
        }

    }


    public static class StoreEncoder {

        public static Result<ByteBuffer> encode(Message message) {
            //---------------基础属性----------------
            byte[] topic = message.getTopic().getBytes(StandardCharsets.UTF_8);
            byte topicLen = (byte) topic.length;
            int partitionId = message.getPartition();
            byte[] properties = CommonUtil.messageProperties2String(message.getProperties()).getBytes(StandardCharsets.UTF_8);
            int propertiesLen = properties.length;
            //now used GZIP compress
            //todo get compress way in properties
            Result<byte[]> compressResult = CompressUtil.GZIP.compress(message.getBody());
            if (!compressResult.success()) {
                return Results.failure(compressResult.getMsg());
            }
            byte[] body = compressResult.getData();
            int bodyLen = body.length;
            //---------------存储附加属性----------------
            long offset = message.getOffset();
            long storeTimestamp = message.getStoreTimestamp();
            int crc = message.getCrc();
            byte version = message.getVersion();
            //this storeLen is after offset
            int storeLen = CommonUtil.calStoreLength(bodyLen, topicLen, propertiesLen);

            //------ build append buffer ----
            ByteBuffer storeByteBuffer = ByteBuffer.allocate(LogConstants.MessageKeyBytes.LOG_OVERHEAD + storeLen);
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
            //复位position，append根据position写入。
            storeByteBuffer.flip();
            return Results.success(storeByteBuffer);
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
                Map<String, String> properties = CommonUtil.string2messageProperties(new String(propertiesBytes, StandardCharsets.UTF_8));
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
                Result<byte[]> uncompressResult = CompressUtil.GZIP.uncompress(body);
                if (!uncompressResult.success()) {
                    return Results.failure(uncompressResult.getMsg());
                }
                message.setBody(uncompressResult.getData());
                return Results.success(message);
            } catch (Throwable e) {
                logger.error("store log message decode error.", e);
                return Results.failure("store log message decode error.");
            }
        }

    }


}
