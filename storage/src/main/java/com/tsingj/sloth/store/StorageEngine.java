package com.tsingj.sloth.store;

import com.tsingj.sloth.store.log.Log;
import com.tsingj.sloth.store.properties.StorageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 日志存储与读取入口
 *
 * @author yanghao
 */

@Service
public class StorageEngine implements Storage {

    private static final Logger logger = LoggerFactory.getLogger(StorageEngine.class);

    private final Log log;

    private final StorageProperties storageProperties;


    public StorageEngine(Log log, StorageProperties storageProperties) {
        this.log = log;
        this.storageProperties = storageProperties;
    }

    @Override
    public boolean load() {
        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public PutMessageResult putMessage(Message message) {

        if (message.getTopic().length() > Byte.MAX_VALUE) {
            String errorMsg = "message topic length too long, max length " + Byte.MAX_VALUE + "!";
            logger.warn(errorMsg);
            return PutMessageResult.builder().status(PutMessageStatus.MESSAGE_ILLEGAL).errorMsg(errorMsg).build();
        }

        if (message.getBody().length > storageProperties.getMessageMaxSize()) {
            String errorMsg = "message body length too long, max length " + storageProperties.getMessageMaxSize() + "!";
            logger.warn(errorMsg);
            return PutMessageResult.builder().status(PutMessageStatus.MESSAGE_ILLEGAL).errorMsg(errorMsg).build();
        }

        long beginTime = System.currentTimeMillis();
        PutMessageResult result = log.putMessage(message);
        long costTime = System.currentTimeMillis() - beginTime;
        if (costTime > CommonConstants.DATA_LOG_STORE_WAIN_TIME) {
            logger.warn("putMessage cost time(ms)={}, bodyLength={}", costTime, message.getBody().length);
        }
        return result;
    }

    @Override
    public GetMessageResult getMessage(String topic, int partition, long offset) {

        if (topic.length() > Byte.MAX_VALUE) {
            String errorMsg = "message topic length too long, max length " + Byte.MAX_VALUE + "!";
            logger.warn(errorMsg);
            return GetMessageResult.builder().status(GetMessageStatus.TOPIC_ILLEGAL).errorMsg(errorMsg).build();
        }

        long beginTime = System.currentTimeMillis();
        GetMessageResult getMessageResult = log.getMessage(topic, partition, offset);
        long costTime = System.currentTimeMillis() - beginTime;
        if (costTime > CommonConstants.DATA_LOG_FIND_WAIN_TIME) {
            logger.warn("getMessage cost time(ms)={}", costTime);
        }
        return getMessageResult;
    }


}
