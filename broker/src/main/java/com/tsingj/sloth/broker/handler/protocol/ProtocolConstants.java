package com.tsingj.sloth.broker.handler.protocol;

/**
 * @author yanghao
 */
public class ProtocolConstants {

    public static final String MAGIC_CODE = "sloth";

    public static class FieldLength {

        public static final int MAGIC_CODE = 5;
        public static final int CORRELATION_ID = 8;
        public static final int VERSION = 1;
        public static final int COMMAND = 1;
        public static final int DATA_LEN = 4;

        public static final int ALL = MAGIC_CODE + CORRELATION_ID + VERSION + COMMAND + DATA_LEN;

    }

}
