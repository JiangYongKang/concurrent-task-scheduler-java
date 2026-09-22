package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler;

import java.util.Map;
import java.util.Optional;

/** taskType -> handler 注册表。 */
public interface TaskHandlerRegistry {

    Optional<TaskHandler> find(String taskType);

    Map<String, TaskHandler> all();
}
