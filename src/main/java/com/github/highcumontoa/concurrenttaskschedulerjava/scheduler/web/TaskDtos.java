package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.web;

import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.AttemptInfo;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.FailureInfo;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.QuotaSnapshot;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.RejectReason;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.SubmitResult;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.TaskRecord;

import java.util.List;

/** Web 层 DTO 集合（仅暴露稳定字段，不泄露内部可变对象）。 */
public final class TaskDtos {

    private TaskDtos() {
    }

    public static class SubmitRequest {
        private String caller;
        private String submitKey;
        private String taskType;
        private String payload;

        public String getCaller() { return caller; }
        public void setCaller(String caller) { this.caller = caller; }
        public String getSubmitKey() { return submitKey; }
        public void setSubmitKey(String submitKey) { this.submitKey = submitKey; }
        public String getTaskType() { return taskType; }
        public void setTaskType(String taskType) { this.taskType = taskType; }
        public String getPayload() { return payload; }
        public void setPayload(String payload) { this.payload = payload; }
    }

    public static class SubmitResponse {
        public String taskId;
        public String submitKey;
        public String caller;
        public String taskType;
        public String status;
        public String verdict;
        public boolean duplicate;
        public String rejectReason;
        public long createdAt;

        public static SubmitResponse from(SubmitResult r) {
            SubmitResponse dto = new SubmitResponse();
            dto.taskId = r.getRecord().getTaskId();
            dto.submitKey = r.getRecord().getSubmitKey();
            dto.caller = r.getRecord().getCaller();
            dto.taskType = r.getRecord().getTaskType();
            dto.status = r.getRecord().getStatus().name();
            dto.verdict = r.getVerdict().name();
            dto.duplicate = r.isDuplicate();
            dto.rejectReason = r.getRejectReason() == null ? null : r.getRejectReason().name();
            dto.createdAt = r.getRecord().getCreatedAt();
            return dto;
        }
    }

    public static class TaskResponse {
        public String taskId;
        public String submitKey;
        public String caller;
        public String taskGroup;
        public String taskType;
        public String status;
        public long createdAt;
        public long updatedAt;
        public long queuedAt;
        public long startedAt;
        public long finishedAt;
        public int attemptCount;
        public int maxAttempts;
        public long nextRunAt;
        public FailureDto failure;
        public List<AttemptDto> attempts;

        public static TaskResponse from(TaskRecord r) {
            TaskResponse d = new TaskResponse();
            d.taskId = r.getTaskId();
            d.submitKey = r.getSubmitKey();
            d.caller = r.getCaller();
            d.taskGroup = r.getTaskGroup();
            d.taskType = r.getTaskType();
            d.status = r.getStatus().name();
            d.createdAt = r.getCreatedAt();
            d.updatedAt = r.getUpdatedAt();
            d.queuedAt = r.getQueuedAt();
            d.startedAt = r.getStartedAt();
            d.finishedAt = r.getFinishedAt();
            d.attemptCount = r.getAttemptCount();
            d.maxAttempts = r.getMaxAttempts();
            d.nextRunAt = r.getNextRunAt();
            d.failure = r.getFailure() == null ? null : FailureDto.from(r.getFailure());
            d.attempts = r.getAttempts().stream().map(AttemptDto::from).toList();
            return d;
        }
    }

    public static class FailureDto {
        public String code;
        public boolean retryable;
        public String message;
        public long failedAt;

        public static FailureDto from(FailureInfo f) {
            FailureDto d = new FailureDto();
            d.code = f.getCode().name();
            d.retryable = f.isRetryable();
            d.message = f.getMessage();
            d.failedAt = f.getFailedAt();
            return d;
        }
    }

    public static class AttemptDto {
        public int attemptNo;
        public long startedAt;
        public long finishedAt;
        public boolean success;
        public String failureCode;
        public String errorMessage;

        public static AttemptDto from(AttemptInfo a) {
            AttemptDto d = new AttemptDto();
            d.attemptNo = a.getAttemptNo();
            d.startedAt = a.getStartedAt();
            d.finishedAt = a.getFinishedAt();
            d.success = a.isSuccess();
            d.failureCode = a.getFailureCode() == null ? null : a.getFailureCode().name();
            d.errorMessage = a.getErrorMessage();
            return d;
        }
    }

    public static class QuotaResponse {
        public String scope;
        public int maxConcurrency;
        public int running;
        public int maxQueued;
        public int queued;
        public int rateLimitPerSecond;
        public double availableTokens;

        public static QuotaResponse from(String scope, QuotaSnapshot s) {
            QuotaResponse d = new QuotaResponse();
            d.scope = scope;
            d.maxConcurrency = s.getMaxConcurrency();
            d.running = s.getRunning();
            d.maxQueued = s.getMaxQueued();
            d.queued = s.getQueued();
            d.rateLimitPerSecond = s.getRateLimitPerSecond();
            d.availableTokens = Math.round(s.getAvailableTokens() * 100.0) / 100.0;
            return d;
        }
    }

    public static class ErrorResponse {
        public String code;
        public String message;

        public ErrorResponse() {
        }

        public ErrorResponse(String code, String message) {
            this.code = code;
            this.message = message;
        }
    }

    /** 仅供文档引用，避免未使用告警：拒绝原因枚举透传名。 */
    static String rejectReasonName(RejectReason r) {
        return r.name();
    }
}
