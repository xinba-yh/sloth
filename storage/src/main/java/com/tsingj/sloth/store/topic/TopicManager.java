package com.tsingj.sloth.store.topic;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.tsingj.sloth.store.DataRecovery;
import com.tsingj.sloth.store.log.LogRecoveryException;
import com.tsingj.sloth.store.pojo.Result;
import com.tsingj.sloth.store.pojo.Results;
import com.tsingj.sloth.store.utils.StoragePathHelper;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author yanghao
 */
@Component
public class TopicManager implements DataRecovery {


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

    @PostConstruct
    @Override
    public void load() {
        logger.info("-------------------------------------------------prepare load topic----------------------------------------------------------------");
        String topicPath = storagePathHelper.getTopicPath();
        //将现有topic文件，copy复制到内存
        File topicFile = new File(topicPath);
        if (!topicFile.exists() || topicFile.length() == 0) {
            logger.info("topics.json not exist or nil, skip initialization.");
            return;
        }
        try {
            String topicJsonStr = FileUtils.readFileToString(topicFile, Charset.defaultCharset());
            ConcurrentHashMap<String, TopicConfig> topicData = JSON.parseObject(topicJsonStr, TOPIC_MAP_TYPE);
            if (topicData != null && topicData.size() > 0) {
                TOPIC_MAP.putAll(topicData);
                logger.info("load topic from topics.json done. size:{}", TOPIC_MAP.size());
            }
        } catch (IOException e) {
            throw new LogRecoveryException("topic recovery exception!", e);
        }
        logger.info("-------------------------------------------------load topic over----------------------------------------------------------------");
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
        return persist();
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


    /**
     * json方式持久化topicMap
     *
     * @return
     */
    private synchronized Result persist() {
        try {
            String configDirPath = storagePathHelper.getConfigDir();
            File configDir = new File(configDirPath);
            if (!configDir.exists()) {
                boolean mkdirs = configDir.mkdirs();
                if (!mkdirs) {
                    return Results.failure("init config dir fail!");
                }
            }
            String topicPath = storagePathHelper.getTopicPath();
            //1、将现有文件copy至.bak文件
            File prevFile = new File(topicPath);
            if (prevFile.exists()) {
                File bakTopicConfigFile = new File(topicPath + ".bak");
                //check exists 防止脏文件。
                if (bakTopicConfigFile.exists()) {
                    boolean delete = bakTopicConfigFile.delete();
                    if (!delete) {
                        return Results.failure("del topics.json.bak fail!");
                    }
                }
                boolean success = bakTopicConfigFile.createNewFile();
                if (!success) {
                    return Results.failure("create topics.json.bak fail!");
                }
                FileUtils.copyFile(prevFile, bakTopicConfigFile);
            }

            //2、将最新数据写入tmp文件
            String data = JSON.toJSONString(TOPIC_MAP, true);
            File tmpTopicConfigFile = new File(topicPath + ".tmp");
            FileUtils.write(tmpTopicConfigFile, data, Charset.defaultCharset());

            //3、删除现有文件，并将tmp文件命名为topics.json
            //首次插入，不存在现有文件。
            if (prevFile.exists()) {
                boolean delete = prevFile.delete();
                if (!delete) {
                    return Results.failure("del topics.json fail!");
                }
            }
            boolean rename = tmpTopicConfigFile.renameTo(new File(topicPath));
            if (!rename) {
                return Results.failure("rename topics.json.tmp to topics.json fail!");
            }
        } catch (IOException e) {
            logger.error("topics.json persist fail!", e);
            return Results.failure("topics.json persist fail!" + e.getMessage());
        }
        return Results.success();
    }


}
