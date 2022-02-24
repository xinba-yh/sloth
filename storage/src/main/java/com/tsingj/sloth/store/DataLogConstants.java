package com.tsingj.sloth.store;

/**
 * @author yanghao
 */
public class DataLogConstants {

    public static class KeyBytes {
        /**
         * offset长度
         */
        public static final int OFFSET = 8;

        /**
         * 存储长度
         */
        public static final int STORE = 4;

        /**
         * 存储时间戳
         */
        public static final int STORE_TIMESTAMP = 8;

        /**
         * 消息长度
         */
        public static final int BODY_SIZE = 4;

        /**
         * 版本
         */
        public static final int VERSION = 1;


        /**
         * crc
         */
        public static final int CRC = 4;

        /**
         * topic长度
         */
        public static final int TOPIC = 1;

        /**
         * 属性长度
         */
        public static final int PROPERTIES = 4;

        /**
         * partitionId长度
         */
        public static final int PARTITiON = 4;
    }


}
