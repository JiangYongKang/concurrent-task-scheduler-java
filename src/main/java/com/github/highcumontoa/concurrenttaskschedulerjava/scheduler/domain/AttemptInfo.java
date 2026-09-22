package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain;

/**
 * 单次执行尝试的结果记录（用于失败原因解释与审计）。
 */
public class AttemptInfo {

    private int attemptNo;
    private long startedAt;
    private long finishedAt;
    private boolean success;
    private FailureCode failureCode;
    private String errorMessage;

    public AttemptInfo() {
    }

    public AttemptInfo(int attemptNo, long startedAt, long finishedAt,
                       boolean success, FailureCode failureCode, String errorMessage) {
        this.attemptNo = attemptNo;
        this.startedAt = startedAt;
        this.finishedAt = finishedAt;
        this.success = success;
        this.failureCode = failureCode;
        this.errorMessage = errorMessage;
    }

    public int getAttemptNo() { return attemptNo; }
    public void setAttemptNo(int attemptNo) { this.attemptNo = attemptNo; }
    public long getStartedAt() { return startedAt; }
    public void setStartedAt(long startedAt) { this.startedAt = startedAt; }
    public long getFinishedAt() { return finishedAt; }
    public void setFinishedAt(long finishedAt) { this.finishedAt = finishedAt; }
    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }
    public FailureCode getFailureCode() { return failureCode; }
    public void setFailureCode(FailureCode failureCode) { this.failureCode = failureCode; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
}
