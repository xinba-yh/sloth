package com.tsingj.sloth.rpcmodel.protocol;

import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;

/**
 * @author yanghao
 */
public class Ping {

    @Protobuf
    private String ping;

    public String getPing() {
        return ping;
    }

    public void setPing(String ping) {
        this.ping = ping;
    }
}
