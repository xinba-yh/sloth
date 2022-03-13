package com.tsingj.sloth.rpcmodel;

import com.baidu.jprotobuf.pbrpc.ProtobufRPC;
import com.tsingj.sloth.rpcmodel.protocol.Message;
import com.tsingj.sloth.rpcmodel.protocol.Ping;
import com.tsingj.sloth.rpcmodel.protocol.Pong;
import com.tsingj.sloth.rpcmodel.protocol.SendResp;

/**
 * @author yanghao
 */
public interface ClusterService {

    @ProtobufRPC(serviceName = "clusterService", methodName = "heartbeat", onceTalkTimeout = 200)
    Pong heartbeat(Ping ping);

}
