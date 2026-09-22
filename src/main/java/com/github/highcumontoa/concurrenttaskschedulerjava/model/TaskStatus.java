package com.github.highcumontoa.concurrenttaskschedulerjava.model;

/**
 * 任务生命周期状态。
 *
 * <p>状态转移方向（不得回退）：
 * <pre>
 *   QUEUED -> RUNNING -> SUCCEEDED
 *                     |-> FAILED (含重试耗尽/不可重试/执行超时)
 *                     |-> CANCELLED
 *   QUEUED -> CANCELLED (排队中取消)
 * </pre>
 * PENDING_RETRY 是 RUNNING 失败后等待退避的中间态，可再次进入 RUNNING。
 */
public enum TaskStatus {
    /** 已提交并持久化，排队等待配额。 */
    QUEUED,
    /** 已获得配额，正在执行。 */
    RUNNING,
    /** 一次执行失败，按退避策略等待重试。 */
    PENDING_RETRY,
    /** 成功完成（终态）。 */
    SUCCEEDED,
    /** 最终失败：重试耗尽、不可重试错误或执行超时（终态）。 */
    FAILED,
    /** 被调用方取消（终态）。 */
    CANCELLED
}
