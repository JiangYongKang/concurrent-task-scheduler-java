package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain;

/**
 * 失败明细：稳定的原因码、是否可重试、人类可读描述。
 * 随任务记录持久化，保证重启后失败原因仍可解释。
 */
public class FailureInfo {

    private FailureCode code;
    private boolean retryable;
    private String message;
    private long failedAt;

    public FailureInfo() {
    }

    public FailureInfo(FailureCode code, boolean retryable, String message, long failedAt) {
        this.code = code;
        this.retryable = retryable;
        this.message = message;
        this.failedAt = failedAt;
    }

    public FailureCode getCode() { return code; }
    public void setCode(FailureCode code) { this.code = code; }
    public boolean isRetryable() { return retryable; }
    public void setRetryable(boolean retryable) { this.retryable = retryable; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public long getFailedAt() { return failedAt; }
    public void setFailedAt(long failedAt) { this.failedAt = failedAt; }
}
