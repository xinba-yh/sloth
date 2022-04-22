package com.tsingj.sloth.store;

import com.alipay.sofa.jraft.entity.Task;
import com.tsingj.sloth.common.ProtoStuffSerializer;
import com.tsingj.sloth.common.SystemClock;
import com.tsingj.sloth.store.constants.CommonConstants;
import com.tsingj.sloth.store.constants.LogConstants;
import com.tsingj.sloth.store.datalog.DataLog;
import com.tsingj.sloth.store.pojo.*;
import com.tsingj.sloth.store.properties.StorageProperties;
import com.tsingj.sloth.store.replication.LogClosure;
import com.tsingj.sloth.store.replication.LogOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;

/**
 * @author yanghao
 * 对外提供日志存储和获取入口
 */

@Service
public class StorageEngine implements Storage {

    private static final Logger logger = LoggerFactory.getLogger(StorageEngine.class);

    private final DataLog dataLog;

    private final StorageProperties storageProperties;

    private final RaftReplicationServer raftReplicationServer;

    public StorageEngine(DataLog dataLog, StorageProperties storageProperties, RaftReplicationServer raftReplicationServer) {
        this.dataLog = dataLog;
        this.storageProperties = storageProperties;
        this.raftReplicationServer = raftReplicationServer;
    }


    @Override
    public PutMessageResult putMessage(Message message) {

        if (message.getTopic().length() > Byte.MAX_VALUE) {
            String errorMsg = "message topic length too long, max length " + Byte.MAX_VALUE + "!";
            logger.warn(errorMsg);
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, errorMsg);
        }

        if (message.getBody().length > storageProperties.getMessageMaxSize()) {
            String errorMsg = "message body length too long, max length " + storageProperties.getMessageMaxSize() + "!";
            logger.warn(errorMsg);
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, errorMsg);
        }

        long beginTime = SystemClock.now();
        boolean raft = storageProperties.getMode().equals(LogConstants.StoreMode.RAFT);
        PutMessageResult result;
        if (raft) {
            //定义log operation
            LogOperation logOperation = LogOperation.createPutMessageOp(message);
            //定义log callback
            LogClosure logClosure = new LogClosure(logOperation, 3000);
            //定义raft task
            Task task = new Task();
            task.setData(ByteBuffer.wrap(ProtoStuffSerializer.serialize(logOperation)));
            task.setDone(logClosure);
            raftReplicationServer.getNode().apply(task);
            //wait process
            logClosure.waitTimeMills();
            //超时，返回副本异常。
            if (logClosure.timeouted()) {
                result = new PutMessageResult(PutMessageStatus.LOG_FILE_REPLICA_FAIL, logClosure.getErrMsg());
            }
            //非超时，返回执行结果。
            else {
                result = ProtoStuffSerializer.deserialize(logClosure.getResponse(), PutMessageResult.class);
            }
        } else {
            result = dataLog.putMessage(message);
        }
        long costTime = SystemClock.now() - beginTime;
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
            return new GetMessageResult(GetMessageStatus.TOPIC_ILLEGAL, errorMsg);
        }
        long beginTime = SystemClock.now();
        GetMessageResult getMessageResult = dataLog.getMessage(topic, partition, offset);
        long costTime = SystemClock.now() - beginTime;
        if (costTime > CommonConstants.DATA_LOG_FIND_WAIN_TIME) {
            logger.warn("getMessage cost time(ms)={}", costTime);
        }
        return getMessageResult;
    }

    @Override
    public long getMaxOffset(String topic, int partitionId) {
        return dataLog.getMaxOffset(topic, partitionId);
    }

    @Override
    public long getMinOffset(String topic, int partitionId) {
        return dataLog.getMinOffset(topic, partitionId);
    }


}
