package com.tsingj.sloth.broker.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.tsingj.sloth.store.datajson.AbstractCachePersistence;
import com.tsingj.sloth.store.utils.StoragePathHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.lang.reflect.Type;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author yanghao
 */
@Component
public class ConsumerGroupOffsetManager extends AbstractCachePersistence {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerGroupOffsetManager.class);


    private final StoragePathHelper storagePathHelper;

    public ConsumerGroupOffsetManager(StoragePathHelper storagePathHelper) {
        this.storagePathHelper = storagePathHelper;
    }

    private static final String TOPIC_GROUP_SEPARATOR = "@";
    /**
     * topic@group -> partition - offset
     */
    private final static ConcurrentMap<String, ConcurrentMap<Integer, Long>> TOPIC_GROUP_OFFSETS = new ConcurrentHashMap<>(8);

    private final static Type OFFSETS_TYPE = new TypeReference<ConcurrentMap<String, ConcurrentMap<Integer, Long>>>() {
    }.getType();

    //----------------------------------------------------------loadLogs--------------------------------------------------------------------

    @Override
    protected String getFilePath() {
        return storagePathHelper.getConsumerOffsetsPath();
    }

    @Override
    protected void decode(String content) {
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> topicGroupOffsets = JSON.parseObject(content, OFFSETS_TYPE);
        if (topicGroupOffsets == null) {
            return;
        }
        if (topicGroupOffsets.size() > 0) {
            TOPIC_GROUP_OFFSETS.putAll(topicGroupOffsets);
        }
        logger.info("load consumerOffset done. size:{}", TOPIC_GROUP_OFFSETS.size());
    }

    @Override
    public String encode() {
        return JSON.toJSONString(TOPIC_GROUP_OFFSETS, true);
    }


    public synchronized void commitOffset(String group, String topic, int partitionId, long offset) {
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        ConcurrentMap<Integer, Long> partitionOffsetMap = TOPIC_GROUP_OFFSETS.computeIfAbsent(key, s -> new ConcurrentHashMap<>(8));
        Long storeOffset = partitionOffsetMap.put(partitionId, offset);
        if (storeOffset != null && storeOffset > offset) {
            logger.warn("commit consumerOffset < current offset. group [{}] topic [{}] partitionId [{}] commit [{}], current [{}].", group, topic, partitionId, offset, storeOffset);
        }
    }

    public long queryOffset(final String group, final String topic, final int partitionId) {
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        ConcurrentMap<Integer, Long> partitionOffsetMap = TOPIC_GROUP_OFFSETS.get(key);
        if (partitionOffsetMap == null) {
            return -1;
        }
        Long offset = partitionOffsetMap.get(partitionId);
        if (offset == null) {
            return -1;
        }
        return offset;
    }


}
