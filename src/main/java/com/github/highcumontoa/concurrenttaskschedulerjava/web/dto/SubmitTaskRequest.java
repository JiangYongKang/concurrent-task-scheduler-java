package com.github.highcumontoa.concurrenttaskschedulerjava.web.dto;

/** 提交任务请求。taskId 为调用方提供的幂等键。 */
public class SubmitTaskRequest {

    private String taskId;
    private String callerId;
    private String group;
    private String taskType;
    private String payload;
    private Long timeoutMillis;

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
    public Long getTimeoutMillis() { return timeoutMillis; }
    public void setTimeoutMillis(Long timeoutMillis) { this.timeoutMillis = timeoutMillis; }
}
