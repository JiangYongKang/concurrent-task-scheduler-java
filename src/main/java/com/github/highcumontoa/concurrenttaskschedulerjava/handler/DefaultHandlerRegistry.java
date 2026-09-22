package com.github.highcumontoa.concurrenttaskschedulerjava.handler;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** 默认处理器注册表：基于 ConcurrentHashMap，重复注册同类型将被拒绝以避免歧义。 */
public class DefaultHandlerRegistry implements HandlerRegistry {

    private final Map<String, TaskHandler> handlers = new ConcurrentHashMap<>();

    @Override
    public void register(TaskHandler handler) {
        if (handler == null || handler.type() == null || handler.type().isBlank()) {
            throw new IllegalArgumentException("handler and its type must not be blank");
        }
        TaskHandler previous = handlers.putIfAbsent(handler.type(), handler);
        if (previous != null) {
            throw new IllegalStateException("handler already registered for type: " + handler.type());
        }
    }

    @Override
    public Optional<TaskHandler> find(String type) {
        return Optional.ofNullable(handlers.get(type));
    }
}
