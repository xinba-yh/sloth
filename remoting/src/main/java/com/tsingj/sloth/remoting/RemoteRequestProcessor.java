package com.tsingj.sloth.remoting;

import com.tsingj.sloth.remoting.protocol.DataPackage;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author yanghao
 */
public interface RemoteRequestProcessor {

    DataPackage process(DataPackage request, ChannelHandlerContext ctx) throws Exception;

    byte getCommand();

}
