package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler;

/** 处理器执行上下文：提供取消信号。 */
public interface TaskContext {

    String getTaskId();

    String getCaller();

    /** 是否已被取消（协作式取消，处理器应周期检查）。 */
    boolean isCancelled();
}
