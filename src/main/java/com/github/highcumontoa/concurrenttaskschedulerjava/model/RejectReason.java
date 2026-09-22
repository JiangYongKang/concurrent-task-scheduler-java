package com.github.highcumontoa.concurrenttaskschedulerjava.model;

/** 任务被拒绝（未进入排队）的原因，保证调用方可区分。 */
public enum RejectReason {
    /** 调用方/任务组并发或单位时间配额不足，且不允许排队或队列已满。 */
    QUOTA_EXHAUSTED,
    /** 排队任务数达到上限。 */
    QUEUE_FULL,
    /** 提交参数非法。 */
    INVALID_REQUEST,
    /** 未知任务类型，无对应处理器。 */
    UNKNOWN_TASK_TYPE
}
