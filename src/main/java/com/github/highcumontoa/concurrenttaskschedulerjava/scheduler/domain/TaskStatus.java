package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain;

/**
 * 任务生命周期状态。
 *
 * <pre>
 * PENDING/QUEUED -> RUNNING -> WAITING_RETRY -> RUNNING ... -> COMPLETED | FAILED | TIMED_OUT
 * PENDING/QUEUED/WAITING_RETRY/RUNNING -> CANCELLED
 * REJECTED（提交时即终态）
 * </pre>
 */
public enum TaskStatus {
    /** 已持久化，尚未纳入派发（恢复瞬态）。 */
    PENDING,
    /** 排队等待配额。 */
    QUEUED,
    /** 正在执行某次尝试。 */
    RUNNING,
    /** 尝试失败，按退避策略等待重试。 */
    WAITING_RETRY,
    /** 成功完成（终态）。 */
    COMPLETED,
    /** 重试耗尽或不可重试错误（终态）。 */
    FAILED,
    /** 被调用方取消（终态）。 */
    CANCELLED,
    /** 执行超时（终态）。 */
    TIMED_OUT,
    /** 提交时配额不足被拒绝（终态）。 */
    REJECTED;

    public boolean isTerminal() {
        return this == COMPLETED
                || this == FAILED
                || this == CANCELLED
                || this == TIMED_OUT
                || this == REJECTED;
    }
}
