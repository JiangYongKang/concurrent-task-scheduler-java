package com.github.highcumontoa.concurrenttaskschedulerjava.retry;

/**
 * 重试策略：分类错误并计算退避时间。
 * 实现需保证确定性（相同输入得到相同结果）。
 */
public interface RetryPolicy {

    /** 最大重试次数（不含首次执行）。 */
    int maxRetries();

    /** 对一次执行异常进行分类。 */
    ErrorClassification classify(Throwable error);

    /**
     * 计算第 attempt 次重试前的退避毫秒数。
     * @param retryIndex 从 1 开始的重试序号
     */
    long backoffMillis(int retryIndex);
}
