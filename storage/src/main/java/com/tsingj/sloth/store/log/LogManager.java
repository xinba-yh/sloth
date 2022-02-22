package com.tsingj.sloth.store.log;


import com.tsingj.sloth.store.utils.CrcUtil;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.FieldDefaults;

import java.nio.ByteBuffer;

/**
 * @author yanghao
 */
public class LogManager {

    //------------------------------以下为固定大小------------------------------

    /**
     * 消息长度
     */
    private static final int MSG_SIZE_LENGTH = 4;

    /**
     * 版本
     */
    private static final int VERSION_LENGTH = 1;


    /**
     * crc
     */
    private static final int CRC_LENGTH = 4;

    /**
     * 当前log版本
     */
    private static final byte VERSION = 1;


    public static byte[] buildLog(byte[] payload) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(MSG_SIZE_LENGTH + VERSION_LENGTH + CRC_LENGTH + payload.length);
        byteBuffer.putInt(payload.length);
        byteBuffer.put(VERSION);
        byteBuffer.putInt(CrcUtil.crc32(payload));
        byteBuffer.put(payload);
        return byteBuffer.array();
    }

    public static LogInfo parseLogInfo(byte[] msgBytes) {
        ByteBuffer wrap = ByteBuffer.wrap(msgBytes);
        //前4位 int类型 msgSize
        int msgSize = wrap.getInt();
        //第5位 byte类型 version
        byte version = wrap.get();
        //第6-9 4位 int类型
        int crc = wrap.getInt();
        //按照msgSize读取指定大小的payload
        byte[] payload = new byte[msgSize];
        wrap.get(payload);
        return LogInfo.builder().msgSize(msgSize).version(version).crc(crc).payload(payload).build();
    }

    @Data
    @Builder
    @FieldDefaults(level = AccessLevel.PRIVATE)
    public static class LogInfo {
        /**
         * payload大小
         */
        int msgSize;
        /**
         * 存储版本号
         */
        byte version;
        /**
         * crc校验码
         */
        int crc;
        /**
         * 消息体
         */
        byte[] payload;
    }

}
