package com.tsingj.sloth.store;

public interface StorageService {

    public void write(String topic,int partition,String data);

}
