package com.github.highcumontoa.concurrenttaskschedulerjava.handler;

/**
 * 任务执行上下文：处理器通过它感知取消信号与本次执行序号。
 * 处理器宜在关键步骤检查 {@link #cancelled()}，以便取消/超时及时停止，避免重复副作用。
 */
public interface TaskContext {

    /** 任务提交标识（幂等键）。 */
    String taskId();

    /** 调用方标识。 */
    String callerId();

    /** 任务组（配额维度）。 */
    String group();

    /** 第几次执行（1 表示首次执行）。 */
    int attempt();

    /** 是否已被取消或超时中断。 */
    boolean cancelled();
}
