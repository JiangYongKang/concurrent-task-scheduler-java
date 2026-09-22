package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain;

/** 提交结果：区分已受理(去重)、排队、拒绝。 */
public class SubmitResult {

    private final TaskRecord record;
    private final boolean duplicate;
    private final QuotaVerdict verdict;
    private final RejectReason rejectReason;

    public SubmitResult(TaskRecord record, boolean duplicate, QuotaVerdict verdict, RejectReason rejectReason) {
        this.record = record;
        this.duplicate = duplicate;
        this.verdict = verdict;
        this.rejectReason = rejectReason;
    }

    public TaskRecord getRecord() { return record; }
    public boolean isDuplicate() { return duplicate; }
    public QuotaVerdict getVerdict() { return verdict; }
    public RejectReason getRejectReason() { return rejectReason; }
}
