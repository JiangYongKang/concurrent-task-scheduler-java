package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain;

import java.util.ArrayList;
import java.util.List;

/**
 * 任务的持久化记录。所有状态变化都先写盘再对外生效。
 * 该类为可变 POJO（便于 Jackson 序列化），并发修改由调度服务统一加锁；
 * 对外暴露时使用 {@link #copy()} 做防御性拷贝。
 */
public class TaskRecord {

    private String taskId;
    private String submitKey;
    private String caller;
    private String taskGroup;
    private String taskType;
    private String payload;
    private TaskStatus status;
    private long createdAt;
    private long updatedAt;
    private long queuedAt;
    private long startedAt;
    private long finishedAt;
    private int attemptCount;
    private int maxAttempts;
    private long nextRunAt;
    private FailureInfo failure;
    private List<AttemptInfo> attempts = new ArrayList<>();

    public TaskRecord() {
    }

    /** 创建一条新任务记录（状态 PENDING）。 */
    public static TaskRecord create(String taskId, String caller, String submitKey,
                                    String taskGroup, String taskType, String payload,
                                    int maxAttempts, long now) {
        TaskRecord r = new TaskRecord();
        r.taskId = taskId;
        r.caller = caller;
        r.submitKey = submitKey;
        r.taskGroup = taskGroup;
        r.taskType = taskType;
        r.payload = payload;
        r.status = TaskStatus.PENDING;
        r.maxAttempts = maxAttempts;
        r.createdAt = now;
        r.updatedAt = now;
        return r;
    }

    /** 深拷贝（含 attempts 列表），用于对外返回与持久化快照。 */
    public TaskRecord copy() {
        TaskRecord c = new TaskRecord();
        c.taskId = taskId;
        c.submitKey = submitKey;
        c.caller = caller;
        c.taskGroup = taskGroup;
        c.taskType = taskType;
        c.payload = payload;
        c.status = status;
        c.createdAt = createdAt;
        c.updatedAt = updatedAt;
        c.queuedAt = queuedAt;
        c.startedAt = startedAt;
        c.finishedAt = finishedAt;
        c.attemptCount = attemptCount;
        c.maxAttempts = maxAttempts;
        c.nextRunAt = nextRunAt;
        c.failure = failure == null ? null
                : new FailureInfo(failure.getCode(), failure.isRetryable(), failure.getMessage(), failure.getFailedAt());
        c.attempts = new ArrayList<>(attempts);
        return c;
    }

    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }
    public String getSubmitKey() { return submitKey; }
    public void setSubmitKey(String submitKey) { this.submitKey = submitKey; }
    public String getCaller() { return caller; }
    public void setCaller(String caller) { this.caller = caller; }
    public String getTaskGroup() { return taskGroup; }
    public void setTaskGroup(String taskGroup) { this.taskGroup = taskGroup; }
    public String getTaskType() { return taskType; }
    public void setTaskType(String taskType) { this.taskType = taskType; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public TaskStatus getStatus() { return status; }
    public void setStatus(TaskStatus status) { this.status = status; }
    public long getCreatedAt() { return createdAt; }
    public void setCreatedAt(long createdAt) { this.createdAt = createdAt; }
    public long getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(long updatedAt) { this.updatedAt = updatedAt; }
    public long getQueuedAt() { return queuedAt; }
    public void setQueuedAt(long queuedAt) { this.queuedAt = queuedAt; }
    public long getStartedAt() { return startedAt; }
    public void setStartedAt(long startedAt) { this.startedAt = startedAt; }
    public long getFinishedAt() { return finishedAt; }
    public void setFinishedAt(long finishedAt) { this.finishedAt = finishedAt; }
    public int getAttemptCount() { return attemptCount; }
    public void setAttemptCount(int attemptCount) { this.attemptCount = attemptCount; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public long getNextRunAt() { return nextRunAt; }
    public void setNextRunAt(long nextRunAt) { this.nextRunAt = nextRunAt; }
    public FailureInfo getFailure() { return failure; }
    public void setFailure(FailureInfo failure) { this.failure = failure; }
    public List<AttemptInfo> getAttempts() { return attempts; }
    public void setAttempts(List<AttemptInfo> attempts) { this.attempts = attempts; }
}
