package com.github.highcumontoa.concurrenttaskschedulerjava.web.dto;

import com.github.highcumontoa.concurrenttaskschedulerjava.model.TaskRecord;

/** 任务视图（对外查询）。 */
public record TaskView(String taskId, String callerId, String group, String taskType,
                       String status, int attempts, long enqueueTime, long startTime,
                       long endTime, String result, String errorReason, String errorClass,
                       long timeoutMillis, long version) {

    public static TaskView from(TaskRecord r) {
        return new TaskView(r.getTaskId(), r.getCallerId(), r.getGroup(), r.getTaskType(),
                r.getStatus() == null ? null : r.getStatus().name(), r.getAttempts(),
                r.getEnqueueTime(), r.getStartTime(), r.getEndTime(), r.getResult(),
                r.getErrorReason(), r.getErrorClass(), r.getTimeoutMillis(), r.getVersion());
    }
}
