package com.tsingj.sloth.store.datalog.checkpoints;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.tsingj.sloth.store.datajson.AbstractCachePersistence;
import com.tsingj.sloth.store.utils.StoragePathHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.lang.reflect.Type;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author yanghao
 */
@Component
public class OffsetCheckpointManager extends AbstractCachePersistence {

    private static final Logger logger = LoggerFactory.getLogger(OffsetCheckpointManager.class);

    private final StoragePathHelper storagePathHelper;

    public OffsetCheckpointManager(StoragePathHelper storagePathHelper) {
        this.storagePathHelper = storagePathHelper;
    }

    /**
     * topic -> (partition -> file flushed offset)
     */
    private final static ConcurrentMap<String, ConcurrentMap<Integer, Long>> TOPIC_PARTITION_CHECKPOINT_OFFSETS = new ConcurrentHashMap<>(8);

    private final static Type STORE_TYPE = new TypeReference<ConcurrentMap<String, ConcurrentMap<Integer, Long>>>() {
    }.getType();

    //----------------------------------------------------------loadLogs--------------------------------------------------------------------

    @Override
    protected String getFilePath() {
        return storagePathHelper.getRecoveryPointOffsetCheckpointPath();
    }

    @Override
    protected void decode(String content) {
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> topicPartitionCheckpointOffsets = JSON.parseObject(content, STORE_TYPE);
        if (topicPartitionCheckpointOffsets == null) {
            return;
        }
        if (topicPartitionCheckpointOffsets.size() > 0) {
            TOPIC_PARTITION_CHECKPOINT_OFFSETS.putAll(topicPartitionCheckpointOffsets);
        }
        logger.info("load topicPartitionCheckpointOffsets done. size:{}", TOPIC_PARTITION_CHECKPOINT_OFFSETS.size());
    }

    @Override
    public String encode() {
        return JSON.toJSONString(TOPIC_PARTITION_CHECKPOINT_OFFSETS, true);
    }


    public void submit(String topic, int partition, long offset) {
        ConcurrentMap<Integer, Long> partitionOffsetMap = TOPIC_PARTITION_CHECKPOINT_OFFSETS.computeIfAbsent(topic, s -> new ConcurrentHashMap<>(8));
        partitionOffsetMap.put(partition, offset);
    }

    public Optional<Long> get(String topic, int partition) {
        ConcurrentMap<Integer, Long> partitionCheckpointOffsets = TOPIC_PARTITION_CHECKPOINT_OFFSETS.get(topic);
        if (partitionCheckpointOffsets == null) {
            return Optional.empty();
        }
        Long offset = partitionCheckpointOffsets.get(partition);
        return Optional.ofNullable(offset);
    }

}
