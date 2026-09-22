package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain;

/** 任务最终失败或单次尝试失败的可解释原因码。 */
public enum FailureCode {
    /** 处理器抛出普通异常（默认可重试）。 */
    HANDLER_EXCEPTION,
    /** 处理器抛出 NonRetryableTaskException，立即终止。 */
    NON_RETRYABLE,
    /** 单次执行超时，任务终态 TIMED_OUT，不重试。 */
    ATTEMPT_TIMEOUT,
    /** 被调用方取消，任务终态 CANCELLED。 */
    CANCELLED,
    /** 进程重启导致 RUNNING 中断（默认判失败，可配置重试）。 */
    RESTART_INTERRUPTED,
    /** 未知任务类型（提交被拒绝/无法执行）。 */
    UNKNOWN_TASK_TYPE,
    /** 请求参数非法。 */
    INVALID_REQUEST,
    /** 调用方队列已满，提交被拒绝。 */
    CALLER_QUEUE_FULL,
    /** 全局队列已满，提交被拒绝。 */
    GLOBAL_QUEUE_FULL
}
