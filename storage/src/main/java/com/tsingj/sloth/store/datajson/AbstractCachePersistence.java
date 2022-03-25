package com.tsingj.sloth.store.datajson;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * @author yanghao
 */
public abstract class AbstractCachePersistence implements CachePersistence {

    private static final Logger logger = LoggerFactory.getLogger(AbstractCachePersistence.class);


    @Override
    public void load() {
        String fileName = this.getFilePath();
        logger.info("-------------------------------------------------prepare load " + fileName + "----------------------------------------------------------------");
        File topicFile = new File(fileName);
        if (!topicFile.exists()) {
            topicFile = new File(fileName + ".bak");
            if (!topicFile.exists()) {
                logger.info(fileName + " not exist, skip initialization.");
                return;
            }
        }
        if (topicFile.length() == 0) {
            logger.info(fileName + " exist but nil, skip initialization.");
            return;
        }
        try {
            String content = FileUtils.readFileToString(topicFile, Charset.defaultCharset());
            this.decode(content);
        } catch (IOException e) {
            throw new CachePersistenceException(e);
        }
        logger.info("-------------------------------------------------load " + fileName + " over----------------------------------------------------------------");
    }

    @Override
    public synchronized void persist() throws IOException {
        String encodeStr = this.encode();
        if (encodeStr == null) {
            return;
        }
        String filePath = this.getFilePath();
        //1、将现有文件copy至.bak文件
        File prevFile = new File(filePath);
        if (prevFile.exists()) {
            File bakTopicConfigFile = new File(filePath + ".bak");
            //check exists 防止脏文件。
            if (bakTopicConfigFile.exists()) {
                boolean delete = bakTopicConfigFile.delete();
                if (!delete) {
                    logger.warn("del " + bakTopicConfigFile.getAbsolutePath() + " fail!");
                    return;
                }
            }
            boolean success = bakTopicConfigFile.createNewFile();
            if (!success) {
                logger.warn("create " + bakTopicConfigFile.getAbsolutePath() + " fail!");
                return;
            }
            FileUtils.copyFile(prevFile, bakTopicConfigFile);
        }

        //2、将最新数据写入tmp文件
        File tmpTopicConfigFile = new File(filePath + ".tmp");
        FileUtils.write(tmpTopicConfigFile, encodeStr, Charset.defaultCharset());

        //3、删除现有文件，并将tmp文件命名为存储文件名称
        //首次插入，不存在现有文件。
        if (prevFile.exists()) {
            boolean delete = prevFile.delete();
            if (!delete) {
                logger.warn("del " + prevFile.getAbsolutePath() + " fail!");
                return;
            }
        }
        boolean rename = tmpTopicConfigFile.renameTo(new File(filePath));
        if (!rename) {
            logger.warn("rename " + tmpTopicConfigFile.getAbsolutePath() + " to " + prevFile.getName() + " fail!");
        }

    }

    protected abstract String getFilePath();

    protected abstract void decode(String data);

    public abstract String encode();


}
