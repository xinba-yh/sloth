package com.tsingj.sloth.client.springsupport;

import lombok.Data;

import java.util.Map;

/**
 * @author yanghao
 */
@Data
public class Consumer {

    private boolean enable;

    private Map<String, ConsumerProperties> consumers;

}
