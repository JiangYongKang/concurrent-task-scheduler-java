package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 默认处理器注册表：Spring 容器中的所有 {@link TaskHandler} Bean 自动注册，
 * 同时支持运行时动态注册（测试场景）。
 */
public class DefaultTaskHandlerRegistry implements TaskHandlerRegistry {

    private static final Logger log = LoggerFactory.getLogger(DefaultTaskHandlerRegistry.class);

    private final Map<String, TaskHandler> handlers = new ConcurrentHashMap<>();

    public DefaultTaskHandlerRegistry(List<TaskHandler> beans) {
        if (beans != null) {
            beans.forEach(this::register);
        }
    }

    @Override
    public Optional<TaskHandler> find(String taskType) {
        return Optional.ofNullable(handlers.get(taskType));
    }

    @Override
    public Map<String, TaskHandler> all() {
        return Map.copyOf(handlers);
    }

    public final void register(TaskHandler handler) {
        TaskHandler old = handlers.put(handler.type(), handler);
        log.info("注册任务处理器: type={} class={} replaced={}",
                handler.type(), handler.getClass().getSimpleName(), old != null);
    }
}
