package com.tsingj.sloth.store.datajson.topic;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.tsingj.sloth.store.datajson.AbstractCachePersistence;
import com.tsingj.sloth.store.pojo.Result;
import com.tsingj.sloth.store.pojo.Results;
import com.tsingj.sloth.store.utils.StoragePathHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author yanghao
 */
@Component
public class TopicManager extends AbstractCachePersistence {


    private static final Logger logger = LoggerFactory.getLogger(TopicManager.class);

    private final StoragePathHelper storagePathHelper;

    public TopicManager(StoragePathHelper storagePathHelper) {
        this.storagePathHelper = storagePathHelper;
    }

    /**
     * record topicConfigs
     */
    public final static ConcurrentHashMap<String, TopicConfig> TOPIC_MAP = new ConcurrentHashMap<>();

    private final Object topicMapLock = new Object();

    private final static Type TOPIC_MAP_TYPE = new TypeReference<ConcurrentHashMap<String, TopicConfig>>() {
    }.getType();

    /**
     * record topic auto partition index.
     */
    private final static ConcurrentHashMap<String, AtomicLong> TOPIC_AUTO_INDEX = new ConcurrentHashMap<>();


    //----------------------------------------------------------loadLogs--------------------------------------------------------------------
    @Override
    protected String getFilePath() {
        return storagePathHelper.getTopicPath();
    }

    @Override
    public void decode(String content) {
        ConcurrentHashMap<String, TopicConfig> topicData = JSON.parseObject(content, TOPIC_MAP_TYPE);
        if (topicData == null) {
            return;
        }
        if (topicData.size() > 0) {
            TOPIC_MAP.putAll(topicData);
        }
        logger.info("load topic from topics.json done. size:{}", TOPIC_MAP.size());
    }

    @Override
    public String encode() {
        return JSON.toJSONString(TOPIC_MAP, true);
    }


    /**
     * 创建或修改Topic
     */
    public Result createOrUpdateTopic(TopicConfig topicConfig) {
        //check topicName
        String topicName = topicConfig.getTopicName();
        if (topicName.length() > Byte.MAX_VALUE) {
            return Results.failure("message topic length too long, max length " + Byte.MAX_VALUE + "!");
        }
        //create topicName && persist
        TOPIC_MAP.put(topicName, topicConfig);
        try {
            this.persist();
        } catch (IOException e) {
            return Results.failure(e.getMessage());
        }
        return Results.success();
    }

    /**
     * 获取指定topic
     */
    public Result<TopicConfig> getTopic(String topicName, boolean autoCreate) {
        TopicConfig topicConfig = TOPIC_MAP.get(topicName);
        if (topicConfig == null) {
            if (autoCreate) {
                synchronized (topicMapLock) {
                    topicConfig = TOPIC_MAP.get(topicName);
                    if (topicConfig == null) {
                        topicConfig = new TopicConfig(topicName);
                        Result topicConfigResult = createOrUpdateTopic(topicConfig);
                        if (topicConfigResult.failure()) {
                            return Results.failure(topicConfigResult.getMsg());
                        }
                    }
                }
            } else {
                return Results.failure("topic:" + topicName + " invalid!");
            }
        }
        return Results.success(topicConfig);
    }

    /**
     * 自动分配partition
     *
     * @param topicConfig
     * @return
     */
    public int autoAssignPartition(TopicConfig topicConfig) {
        String topicName = topicConfig.getTopicName();
        int partition = topicConfig.getPartition();
        AtomicLong partitionIndex = TOPIC_AUTO_INDEX.computeIfAbsent(topicName, key -> new AtomicLong());
        return (int) (partitionIndex.getAndAdd(1) % partition + 1);
    }

}