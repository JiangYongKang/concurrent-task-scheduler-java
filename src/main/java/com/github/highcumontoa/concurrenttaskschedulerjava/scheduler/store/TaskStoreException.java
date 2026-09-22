package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.store;

/** 持久化层失败异常（写盘失败属于不可恢复错误）。 */
public class TaskStoreException extends RuntimeException {
    public TaskStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
