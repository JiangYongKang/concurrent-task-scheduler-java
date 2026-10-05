package com.github.highcumontoa.concurrenttaskschedulerjava.governance;

import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaLimits;

/**
 * 某个治理作用域（见 {@link GovernanceScope}：caller+group / 仅 caller / 仅 group）
 * 的治理状态持久化记录。
 *
 * <p>callerId 为 null 表示按任务组整体治理；group 为 null 表示按调用方整体治理；
 * 两者不能同时为 null。以 JSON Lines 追加到治理 WAL，同一作用域以最新一行为准回放，
 * 因此运行期的配额调整与暂停/恢复状态都能跨进程重启保留。
 *
 * <p>limits 为 null 表示该作用域没有运行期配额覆盖（回落到更宽作用域/配置/默认值）。
 */
public class GovernanceRecord {

    private String callerId;
    private String group;
    /** 是否暂停派发。 */
    private boolean paused;
    /** 运行期配额覆盖；null 表示无覆盖。 */
    private QuotaLimits limits;
    /** 最近一次操作：ADJUST_QUOTA / PAUSE / RESUME。 */
    private String lastOperation;
    /** 最近一次操作内容（人类可读，如 "maxConcurrency=4,rate=2,queued=100"）。 */
    private String lastOperationDetail;
    /** 最近一次操作时间（epoch 毫秒）。 */
    private long updatedAtEpochMillis;

    public String getCallerId() { return callerId; }
    public void setCallerId(String callerId) { this.callerId = callerId; }
    public String getGroup() { return group; }
    public void setGroup(String group) { this.group = group; }
    public boolean isPaused() { return paused; }
    public void setPaused(boolean paused) { this.paused = paused; }
    public QuotaLimits getLimits() { return limits; }
    public void setLimits(QuotaLimits limits) { this.limits = limits; }
    public String getLastOperation() { return lastOperation; }
    public void setLastOperation(String lastOperation) { this.lastOperation = lastOperation; }
    public String getLastOperationDetail() { return lastOperationDetail; }
    public void setLastOperationDetail(String d) { this.lastOperationDetail = d; }
    public long getUpdatedAtEpochMillis() { return updatedAtEpochMillis; }
    public void setUpdatedAtEpochMillis(long t) { this.updatedAtEpochMillis = t; }
}
