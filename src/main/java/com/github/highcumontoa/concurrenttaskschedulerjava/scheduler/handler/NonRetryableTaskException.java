package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler;

/** 标记为不可重试的业务异常。 */
public class NonRetryableTaskException extends RuntimeException {
    public NonRetryableTaskException(String message) {
        super(message);
    }

    public NonRetryableTaskException(String message, Throwable cause) {
        super(message, cause);
    }
}
