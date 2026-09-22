package com.github.highcumontoa.concurrenttaskschedulerjava.model;

import org.springframework.http.HttpStatus;

/** 任务提交/取消/查询等同步操作失败的业务异常，由全局异常处理映射为确定的错误响应。 */
public class TaskException extends RuntimeException {

    private final String code;
    private final HttpStatus status;

    public TaskException(String code, String message) {
        this(code, message, HttpStatus.CONFLICT);
    }

    public TaskException(String code, String message, HttpStatus status) {
        super(message);
        this.code = code;
        this.status = status;
    }

    public String getCode() {
        return code;
    }

    public HttpStatus getStatus() {
        return status;
    }
}
