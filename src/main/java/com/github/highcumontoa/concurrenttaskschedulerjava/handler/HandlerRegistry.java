package com.github.highcumontoa.concurrenttaskschedulerjava.handler;

import java.util.Optional;

/** 任务类型 -> 处理器 的注册表。 */
public interface HandlerRegistry {

    void register(TaskHandler handler);

    Optional<TaskHandler> find(String type);
}
