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
public class GetMessageResult {

    /**
     * 状态
     */
    GetMessageStatus status;

    /**
     * 错误信息
     */
    String errorMsg;


    Message message;

}
