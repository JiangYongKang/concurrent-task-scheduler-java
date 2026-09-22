package com.github.highcumontoa.concurrenttaskschedulerjava.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * 任务的完整持久化记录（WAL 中以 JSON 行保存快照）。
 * 使用可变 POJO 以简化 JSON 序列化；字段由调度器在持锁状态下更新。
 */
public class TaskRecord {

    /** 提交标识（幂等键）。 */
    private String taskId;
    /** 调用方标识（配额维度之一）。 */
    private String callerId;
    /** 任务组（配额维度之一）。 */
    private String group;
    /** 任务类型。 */
    private String taskType;
    /** 业务参数 JSON。 */
    private String payload;
    /** 当前状态。 */
    private TaskStatus status;
    /** 已执行次数（首次执行为 1）。 */
    private int attempts;
    /** 入队时间（毫秒）。 */
    private long enqueueTime;
    /** 本次/上次开始执行时间。 */
    private long startTime;
    /** 结束时间（终态）。 */
    private long endTime;
    /** 下次可调度时间（PENDING_RETRY 退避到期）。 */
    private long nextEligibleTime;
    /** 成功结果。 */
    private String result;
    /** 最后一次失败/取消/超时原因（可解释）。 */
    private String errorReason;
    /** 最后一次异常类型。 */
    private String errorClass;
    /** 执行超时毫秒数（&lt;=0 表示不限）。 */
    private long timeoutMillis;
    /**
     * 瞬态取消信号：仅在当前进程内有效，不参与持久化。
     * 执行线程通过它（配合中断）及时停止；重启后 RUNNING 任务一律重排队，无需恢复。
     */
    private transient volatile boolean cancelRequested;
    /** 版本号，每次状态转移 +1。 */
    private long version;

    public TaskRecord() {
    }

    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }
    public String getCallerId() { return callerId; }
    public void setCallerId(String callerId) { this.callerId = callerId; }
    public String getGroup() { return group; }
    public void setGroup(String group) { this.group = group; }
    public String getTaskType() { return taskType; }
    public void setTaskType(String taskType) { this.taskType = taskType; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public TaskStatus getStatus() { return status; }
    public void setStatus(TaskStatus status) { this.status = status; }
    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }
    public long getEnqueueTime() { return enqueueTime; }
    public void setEnqueueTime(long enqueueTime) { this.enqueueTime = enqueueTime; }
    public long getStartTime() { return startTime; }
    public void setStartTime(long startTime) { this.startTime = startTime; }
    public long getEndTime() { return endTime; }
    public void setEndTime(long endTime) { this.endTime = endTime; }
    public long getNextEligibleTime() { return nextEligibleTime; }
    public void setNextEligibleTime(long nextEligibleTime) { this.nextEligibleTime = nextEligibleTime; }
    public String getResult() { return result; }
    public void setResult(String result) { this.result = result; }
    public String getErrorReason() { return errorReason; }
    public void setErrorReason(String errorReason) { this.errorReason = errorReason; }
    public String getErrorClass() { return errorClass; }
    public void setErrorClass(String errorClass) { this.errorClass = errorClass; }
    public long getTimeoutMillis() { return timeoutMillis; }
    public void setTimeoutMillis(long timeoutMillis) { this.timeoutMillis = timeoutMillis; }
    public boolean isCancelRequested() { return cancelRequested; }
    @JsonIgnore
    public void setCancelRequested(boolean cancelRequested) { this.cancelRequested = cancelRequested; }
    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }
}
