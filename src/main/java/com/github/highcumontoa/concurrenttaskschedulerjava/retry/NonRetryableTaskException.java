package com.github.highcumontoa.concurrenttaskschedulerjava.retry;

/**
 * 处理器显式声明的不可重试错误：调度器立即终止为 FAILED，不再退避重试。
 */
public class NonRetryableTaskException extends RuntimeException {

    public NonRetryableTaskException(String message) {
        super(message);
    }

    public NonRetryableTaskException(String message, Throwable cause) {
        super(message, cause);
    }
}
