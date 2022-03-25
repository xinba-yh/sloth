package com.tsingj.sloth.store.datajson.offset;

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
public class ConsumerOffsetManager extends AbstractCachePersistence {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerOffsetManager.class);


    private final StoragePathHelper storagePathHelper;

    public ConsumerOffsetManager(StoragePathHelper storagePathHelper) {
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


    public void commitOffset(final String group, final String topic, final int partitionId,
                                    final long offset) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        this.commitOffset(key, partitionId, offset);
    }

    private void commitOffset(final String key, final int partitionId, final long offset) {
        ConcurrentMap<Integer, Long> partitionOffsetMap = TOPIC_GROUP_OFFSETS.computeIfAbsent(key, s -> new ConcurrentHashMap<>(8));
        Long storeOffset = partitionOffsetMap.get(partitionId);
        if (storeOffset != null && storeOffset > offset) {
            logger.warn("commit consumerOffset < current offset. key [{}] partitionId [{}] commit [{}], current [{}].", key, partitionId, offset, storeOffset);
            return;
        }
        partitionOffsetMap.put(partitionId, offset);
        TOPIC_GROUP_OFFSETS.put(key, partitionOffsetMap);
    }

    public long queryOffset(final String group, final String topic, final int partitionId) {
        // topic@group
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
