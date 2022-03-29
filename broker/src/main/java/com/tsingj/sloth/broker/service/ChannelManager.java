//package com.tsingj.sloth.broker.service;
//
//import io.netty.channel.Channel;
//import lombok.Data;
//import org.springframework.stereotype.Component;
//
//import java.util.concurrent.ConcurrentHashMap;
//
///**
// * @author yanghao
// */
//@Component
//public class ChannelManager {
//
//    private final ConcurrentHashMap<String,ChannelProxy> CHANNEL_MAP = new ConcurrentHashMap<>();
//
//    public int bindClientId(String clientId, Channel channel) {
//
//        return 0;
//    }
//
//    public ChannelProxy getChannel(String clientId) {
//        return null;
//    }
//
//    public void refreshHeartBeat(String clientId, Channel channel) {
//    }
//
//
//    @Data
//    public static class ChannelProxy {
//
//        private String clientId;
//
//        private Channel channel;
//
//        private long heartBeatTimestamp = System.currentTimeMillis();
//
//        public ChannelProxy(String clientId, Channel channel) {
//            this.clientId = clientId;
//            this.channel = channel;
//        }
//    }
//
//}
