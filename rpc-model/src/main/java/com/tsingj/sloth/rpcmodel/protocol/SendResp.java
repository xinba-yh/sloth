package com.tsingj.sloth.rpcmodel.protocol;

import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;

/**
 * @author yanghao
 */
public class SendResp {

    @Protobuf
    private int retCode;

    @Protobuf
    private String info;

    @Protobuf
    private ResultInfo resultInfo;

    public static class ResultInfo {

        @Protobuf
        private String topic;

        @Protobuf
        private int partition;

        @Protobuf
        private long offset;

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public int getPartition() {
            return partition;
        }

        public void setPartition(int partition) {
            this.partition = partition;
        }

        public long getOffset() {
            return offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }
    }

    public int getRetCode() {
        return retCode;
    }

    public void setRetCode(int retCode) {
        this.retCode = retCode;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public ResultInfo getResultInfo() {
        return resultInfo;
    }

    public void setResultInfo(ResultInfo resultInfo) {
        this.resultInfo = resultInfo;
    }
}
