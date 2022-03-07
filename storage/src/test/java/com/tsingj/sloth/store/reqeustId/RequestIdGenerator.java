package com.tsingj.sloth.store.reqeustId;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicLong;

public class RequestIdGenerator {

    private static final AtomicLong lastId = new AtomicLong();                                         // 自增id，用于requestId的生成过程
    private static final long startTimeStamp = System.currentTimeMillis();                               // 启动加载时的时间戳，用于requestId的生成过程
    private static final String ip = getHostIp(); // 本机ip地址，用于requestId的生成过程

    public static String getReqId() {
        // 规则： hexIp(ip)base36(timestamp)-seq
        return hexIp(ip) + "-" + Long.toString(startTimeStamp, Character.MAX_RADIX) + "-" + lastId.incrementAndGet();
    }

    // 将ip转换为定长8个字符的16进制表示形式：255.255.255.255 -> FFFFFFFF
    private static String hexIp(String ip) {
        StringBuilder sb = new StringBuilder();
        for (String seg : ip.split("\\.")) {
            String h = Integer.toHexString(Integer.parseInt(seg));
            if (h.length() == 1) sb.append("0");
            sb.append(h);
        }
        return sb.toString();
    }

    private static String getHostIp() {
        try {
            InetAddress ip4 = Inet4Address.getLocalHost();
            return ip4.getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException("get Current hostIP fail!");
        }
    }


    public static void main(String[] args) {
        for (int i = 0; i < 20; i++) {
            System.out.println(RequestIdGenerator.getReqId());
        }
    }

}
