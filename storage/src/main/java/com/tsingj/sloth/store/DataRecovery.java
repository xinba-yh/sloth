package com.tsingj.sloth.store;


/**
 * @author yanghao
 * 磁盘数据恢复
 * load阶段，出现异常，停止服务启动，需要将错误信息尽可能的提示出来。
 */
public interface DataRecovery {

    /**
     * 加载磁盘数据至内存
     */
    void load(boolean checkPoint,long offsetCheckpoints);

}
