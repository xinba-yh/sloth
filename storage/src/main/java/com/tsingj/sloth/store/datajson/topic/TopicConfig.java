package com.tsingj.sloth.store.datajson.topic;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yanghao
 */
public class TopicConfig {

    private String topicName;

    private int partition = 8;


    public TopicConfig(String topicName) {
        this.topicName = topicName;
    }

    public TopicConfig(String topicName, int partition) {
        this.topicName = topicName;
        this.partition = partition;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }


    public List<Integer> getAllPartitions() {
        List<Integer> partitions = new ArrayList<>();
        for (int i = 0; i < this.partition; i++) {
            int partition = i + 1;
            partitions.add(partition);
        }
        return partitions;
    }

}
