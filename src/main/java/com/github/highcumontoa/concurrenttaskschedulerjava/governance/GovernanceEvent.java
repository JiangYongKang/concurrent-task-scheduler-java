package com.github.highcumontoa.concurrenttaskschedulerjava.governance;

import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaLimits;

/**
 * 一次运行期治理操作的持久化记录（治理 WAL 中的一行 JSON 快照）。
 *
 * <p>同一 {@link GovernanceScope} 以最后一条事件为准回放，从而保证配额调整与暂停/恢复
 * 在进程重启后仍然生效，不会因重启自动恢复放量。
 *
 * <p>{@code limits} 为 null 表示本次操作未调整限额（暂停/恢复事件）。
 *
 * @param seq 单调递增序号
 * @param timeMillis 操作时间（毫秒）
 * @param kind 作用维度类型名称
 * @param callerId 调用方（GROUP 维度为 null）
 * @param group 任务组（CALLER 维度为 null）
 * @param paused 该维度是否暂停派发
 * @param limits 本次设置的完整限额（null 表示本次未调整限额）
 * @param reason 操作说明
 */
public class GovernanceEvent {

    private long seq;
    private long timeMillis;
    private String kind;
    private String callerId;
    private String group;
    private boolean paused;
    private QuotaLimits limits;
    private String reason;

    public GovernanceEvent() {
    }

    public long getSeq() { return seq; }
    public void setSeq(long seq) { this.seq = seq; }
    public long getTimeMillis() { return timeMillis; }
    public void setTimeMillis(long timeMillis) { this.timeMillis = timeMillis; }
    public String getKind() { return kind; }
    public void setKind(String kind) { this.kind = kind; }
    public String getCallerId() { return callerId; }
    public void setCallerId(String callerId) { this.callerId = callerId; }
    public String getGroup() { return group; }
    public void setGroup(String group) { this.group = group; }
    public boolean isPaused() { return paused; }
    public void setPaused(boolean paused) { this.paused = paused; }
    public QuotaLimits getLimits() { return limits; }
    public void setLimits(QuotaLimits limits) { this.limits = limits; }
    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }
}
