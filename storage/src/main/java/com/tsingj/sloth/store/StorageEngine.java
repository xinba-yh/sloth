package com.tsingj.sloth.store;

import com.tsingj.sloth.store.datalog.DataLog;
import com.tsingj.sloth.store.properties.StorageProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * 日志存储与读取入口
 *
 * @author yanghao
 */
@Slf4j
@Service
public class StorageEngine implements Storage {

    private final DataLog dataLog;

    private final StorageProperties storageProperties;

    public StorageEngine(DataLog dataLog, StorageProperties storageProperties) {
        this.dataLog = dataLog;
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
            log.warn(errorMsg);
            return PutMessageResult.builder().status(PutMessageStatus.MESSAGE_ILLEGAL).errorMsg(errorMsg).build();
        }

        if (message.getBody().length > storageProperties.getMessageMaxSize()) {
            String errorMsg = "message body length too long, max length " + storageProperties.getMessageMaxSize() + "!";
            log.warn(errorMsg);
            return PutMessageResult.builder().status(PutMessageStatus.MESSAGE_ILLEGAL).errorMsg(errorMsg).build();
        }

        long beginTime = System.currentTimeMillis();
        PutMessageResult result = dataLog.putMessage(message);
        long costTime = System.currentTimeMillis() - beginTime;
        if (costTime > CommonConstants.DATA_LOG_STORE_WAIN_TIME) {
            log.warn("putMessage cost time(ms)={}, bodyLength={}", costTime, message.getBody().length);
        }
        return result;
    }

    @Override
    public GetMessageResult getMessage(String topic, int partitionId, long offset) {
        return null;
    }
}
