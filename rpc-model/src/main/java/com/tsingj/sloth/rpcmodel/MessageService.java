package com.tsingj.sloth.rpcmodel;

import com.baidu.jprotobuf.pbrpc.ProtobufRPC;
import com.tsingj.sloth.rpcmodel.protocol.Message;
import com.tsingj.sloth.rpcmodel.protocol.SendResp;

/**
 * @author yanghao
 */
public interface MessageService {

    @ProtobufRPC(serviceName = "messageService", methodName = "send", onceTalkTimeout = 3000)
    SendResp send(Message message);

}
