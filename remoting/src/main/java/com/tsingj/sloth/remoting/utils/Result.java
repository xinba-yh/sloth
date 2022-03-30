package com.tsingj.sloth.remoting.utils;


import java.io.Serializable;


/**
 * 返回结果
 *
 * @param <T>
 */
public class Result<T> implements Serializable {

    private static final long serialVersionUID = 2499433944948002095L;

    /**
     * 状态码：0成功1错误
     */
    private int code;

    /**
     * 业业务码
     */
    private String subCode;

    /**
     * 返回数据
     */
    private T data;

    /**
     * 结果说明
     */
    private String msg;

    public final static int SUCCESS = 0;

    public final static int FAILURE = 1;


    /**
     * 判断code是否成功
     *
     * @return
     */
    public boolean success() {
        return this.code == SUCCESS;
    }

    /**
     * 判断code是否失败
     *
     * @return
     */
    public boolean failure() {
        return this.code == FAILURE;
    }


    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getSubCode() {
        return subCode;
    }

    public void setSubCode(String subCode) {
        this.subCode = subCode;
    }

    @Override
    public String toString() {
        return "code:" + code + " subCode:" + subCode + " msg:" + msg + " data:" + data;
    }
}
