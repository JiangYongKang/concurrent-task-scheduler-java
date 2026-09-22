package com.github.highcumontoa.concurrenttaskschedulerjava.service;

import com.github.highcumontoa.concurrenttaskschedulerjava.model.RejectReason;
import com.github.highcumontoa.concurrenttaskschedulerjava.model.TaskStatus;

/**
 * 提交结果：区分“已接受（排队/执行中）”“重复提交”“拒绝”。
 */
public record SubmitResult(boolean accepted, boolean duplicate, String taskId,
                           TaskStatus status, RejectReason rejectReason, String message) {

    public static SubmitResult accepted(String taskId, TaskStatus status) {
        return new SubmitResult(true, false, taskId, status, null, null);
    }

    public static SubmitResult duplicate(String taskId, TaskStatus currentStatus) {
        return new SubmitResult(false, true, taskId, currentStatus, null, "duplicate submission");
    }

    public static SubmitResult rejected(RejectReason reason, String message) {
        return new SubmitResult(false, false, null, null, reason, message);
    }
}
