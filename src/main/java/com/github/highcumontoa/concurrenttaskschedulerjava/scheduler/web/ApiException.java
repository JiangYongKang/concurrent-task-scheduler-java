package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.web;

/** 携带 HTTP 状态码的 API 异常。 */
public class ApiException extends RuntimeException {

    private final int status;

    public ApiException(int status, String message) {
        super(message);
        this.status = status;
    }

    public int getStatus() {
        return status;
    }
}
