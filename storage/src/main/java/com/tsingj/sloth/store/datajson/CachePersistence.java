package com.tsingj.sloth.store.datajson;

import java.io.IOException;

/**
 * @author yanghao
 */
public interface CachePersistence {

    /**
     * 加载磁盘数据至内存
     */
    void load();

    /**
     * 持久化内存文件至磁盘
     */
    void persist() throws IOException;


}
