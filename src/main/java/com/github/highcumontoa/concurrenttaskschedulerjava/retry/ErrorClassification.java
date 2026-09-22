package com.github.highcumontoa.concurrenttaskschedulerjava.retry;

/** 一次执行错误的分类，决定后续是重试、终止还是取消/超时隔离。 */
public enum ErrorClassification {
    /** 可重试错误（受最大重试次数与退避约束）。 */
    RETRYABLE,
    /** 不可重试错误：立即终止为 FAILED。 */
    NON_RETRYABLE,
    /** 执行超时：不重试，立即终止为 FAILED。 */
    TIMEOUT,
    /** 执行期间被取消：立即终止为 CANCELLED。 */
    CANCELLED
}
