
package com.tsingj.sloth.store.replication;

import com.tsingj.sloth.common.ProtoStuffSerializer;
import com.tsingj.sloth.store.pojo.Message;

import java.io.Serializable;


/**
 * @author yanghao
 */
public class LogOperation implements Serializable {

    private static final long serialVersionUID = -6597003954824547294L;


    public final static byte APPEND_MESSAGE = 1;

    public final static byte GET_MESSAGE = 2;


    private final byte type;
    private final byte[] data;

    public static LogOperation createPutMessageOp(Message data) {
        return new LogOperation(APPEND_MESSAGE, ProtoStuffSerializer.serialize(data));
    }

    public LogOperation(byte type, byte[] data) {
        this.type = type;
        this.data = data;
    }

    public byte getType() {
        return type;
    }

    public byte[] getData() {
        return data;
    }
}
