package com.tsingj.sloth.store;

/**
 * @author yanghao
 */
public class DataLogConstants {

    public static class FileSuffix {

        public final static String LOG = ".log";

        public final static String OFFSET_INDEX = ".index";

        public final static String TIMESTAMP_INDEX = ".timeindex";

    }

    public static class MessageKeyBytes{

        /**
         * offset长度
         */
        public final static int OFFSET = 8;

        /**
         * 存储长度
         */
        public final static int STORE = 4;

        /**
         *  消息头长度
         */
        public final static int LOG_OVERHEAD = OFFSET + STORE;

        /**
         * 存储时间戳
         */
        public final static int STORE_TIMESTAMP = 8;

        /**
         * 消息长度
         */
        public final static int BODY_SIZE = 4;

        /**
         * 版本
         */
        public final static int VERSION = 1;


        /**
         * crc
         */
        public final static int CRC = 4;

        /**
         * topic长度
         */
        public final static int TOPIC = 1;

        /**
         * 属性长度
         */
        public final static int PROPERTIES = 4;

        /**
         * partitionId长度
         */
        public final static int PARTITION = 4;
        

    }


    public static final int INDEX_BYTES = 16;

    public static class IndexKeyBytes {
       
        public final static int KEY = 8;
        
        public final static int VALUE = 8;
    }


}
