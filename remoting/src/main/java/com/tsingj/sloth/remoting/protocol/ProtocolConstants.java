package com.tsingj.sloth.remoting.protocol;

/**
 * @author yanghao
 */
public class ProtocolConstants {

    public static final String MAGIC_CODE = "sloth";

    public static final byte VERSION = 1;

    public static class Command {
        public static final byte HEARTBEAT = 1;
        public static final byte SEND_MESSAGE = 2;
    }

    public static class FieldLength {

        /**
         * head
         */
        public static final int MAGIC_CODE = 5;
        public static final int VERSION = 1;
        public static final int COMMAND = 1;
        public static final int TOTAL_LEN = 4;
        public static final int HEAD_ALL = MAGIC_CODE + VERSION + COMMAND + TOTAL_LEN;

        /**
         * meta
         */
        public static final int REQUEST_TYPE = 1;
        public static final int CORRELATION_ID = 8;
        public static final int TIMESTAMP = 8;

        /**
         * data
         */
        public static final int DATA_LEN = 4;
    }

    public static class RequestType {
        public static final byte ONE_WAY = 0;
        public static final byte SYNC = 1;
    }

}
