package com.github.highcumontoa.concurrenttaskschedulerjava.web.dto;

import com.github.highcumontoa.concurrenttaskschedulerjava.model.RejectReason;
import com.github.highcumontoa.concurrenttaskschedulerjava.model.TaskStatus;

/** 提交响应：accepted/duplicate/rejected 三态可区分。 */
public record SubmitTaskResponse(boolean accepted, boolean duplicate, String taskId,
                                 TaskStatus status, RejectReason rejectReason, String message) {
}
