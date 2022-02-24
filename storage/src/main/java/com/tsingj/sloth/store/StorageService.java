package com.tsingj.sloth.store;


/**
 * 1、创建| 获取文件
 * 2、追加消息
 * 3、获取消息
 */
public interface StorageService {

    public void write(String topic,int partition,String data);

}
