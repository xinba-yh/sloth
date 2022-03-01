package com.tsingj.sloth.store.pojo;


public class Results<T> {

    private static String DEFAULT_SUCCESS_MESSAGE = "成功";

    private Results(){}

    public static <T> Result<T> success() {
        return success(null, DEFAULT_SUCCESS_MESSAGE);
    }

    public static <T> Result<T> success(T data) {
        return success(data, DEFAULT_SUCCESS_MESSAGE);
    }

    public static <T> Result<T> success(T data, String message) {
        Result<T> result = new Result<>();
        result.setCode(0);
        result.setData(data);
        result.setMsg(message);
        return result;
    }

    public static <T> Result<T> failure(String msg) {
        return failure(null, msg, null);
    }

    public static <T> Result<T> failure(String subCode, String msg) {
        return failure(subCode, msg, null);
    }

    public static <T> Result<T> failure(String subCode, String msg, T data) {
        Result<T> result = new Result<>();
        result.setCode(1);
        result.setData(data);
        result.setSubCode(subCode);
        result.setMsg(msg);
        return result;
    }

    public static <T> Result<T> warn(String msg, int code) {
        return warn(msg, code, null);
    }

    public static <T> Result<T> warn(String msg, int code, T data) {
        Result<T> result = new Result<>();
        result.setCode(code);
        result.setMsg(msg);
        result.setData(data);
        return result;
    }

}
