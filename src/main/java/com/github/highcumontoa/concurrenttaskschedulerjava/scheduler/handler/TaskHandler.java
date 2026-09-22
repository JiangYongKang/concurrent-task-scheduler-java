package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler;

/**
 * 任务处理器。按 taskType 注册。
 */
public interface TaskHandler {

    String type();

    /**
     * 执行任务。
     *
     * @throws NonRetryableTaskException 抛出后不再重试
     */
    void handle(String payload, TaskContext context) throws Exception;
}
