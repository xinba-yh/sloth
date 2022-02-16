package com.tsingj.sloth.store;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.FieldDefaults;

/**
 * @author yanghao
 */
@Data
@Builder
@FieldDefaults(level = AccessLevel.PRIVATE)
public class Message {

    Long id;

    String data;

    Long createTime;

}
