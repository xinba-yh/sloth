package com.tsingj.sloth.remoting;

import com.tsingj.sloth.remoting.protocol.RemoteCommand;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author yanghao
 */
public interface RemoteRequestProcessor {

    RemoteCommand process(RemoteCommand request, ChannelHandlerContext ctx) throws Exception;

    byte getCommand();

}
