package com.tsingj.sloth.client.springsupport;

import lombok.Data;

/**
 * @author yanghao
 */
@Data
public class ProducerProperties {

    private boolean enable;

    /**
     * 每次超时时间,默认：1000ms
     */
    private int timeout = 1000;

    /**
     * 重试次数,默认：0
     */
    private int retryTimes = 0;

    /**
     * 重试间隔,默认:200ms
     */
    private int retryInterval = 200;

    /**
     * 消息最大字节数，默认：1M
     */
    private int messageMaxBytes = 1024 * 1024;

}
