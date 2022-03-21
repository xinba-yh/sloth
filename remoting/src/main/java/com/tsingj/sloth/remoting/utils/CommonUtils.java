package com.tsingj.sloth.remoting.utils;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import lombok.extern.slf4j.Slf4j;

/**
 * @author yanghao
 */
@Slf4j
public class CommonUtils {

    public static String simpleErrorInfo(final Throwable e) {

        StringBuilder sb = new StringBuilder();
        if (e != null) {
            sb.append(e.toString());
            StackTraceElement[] stackTrace = e.getStackTrace();
            if (stackTrace != null && stackTrace.length > 0) {
                StackTraceElement elment = stackTrace[0];
                sb.append(", ");
                sb.append(elment.toString());
            }
        }
        return sb.toString();
    }


    public static void closeChannel(Channel channel, String message) {
        if (null == channel) {
            return;
        }
        channel.close().addListener((ChannelFutureListener) future -> log.info("close connection [{}] ,result: {}", message, future.isSuccess()));
    }


}
