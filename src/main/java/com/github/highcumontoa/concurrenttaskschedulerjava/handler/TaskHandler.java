package com.github.highcumontoa.concurrenttaskschedulerjava.handler;

/**
 * 任务处理器：按任务类型注册。
 * 实现应尽量在成功返回前保证副作用幂等；框架保证同一任务在任一时刻只有一个执行实例。
 */
public interface TaskHandler {

    /** 任务类型标识。 */
    String type();

    /**
     * 执行任务。
     * @param payload 提交时携带的 JSON 参数
     * @param ctx 执行上下文
     * @return 执行结果（将被持久化，可用于查询）
     */
    String execute(String payload, TaskContext ctx) throws Exception;
}
