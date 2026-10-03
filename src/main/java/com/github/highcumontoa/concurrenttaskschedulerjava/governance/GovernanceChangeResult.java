package com.github.highcumontoa.concurrenttaskschedulerjava.governance;

import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaLimits;

/**
 * 一次治理变更（配额调整 / 暂停 / 恢复）的结果与该作用域当前状态。
 *
 * @param scope 本次操作的作用域
 * @param paused 该作用域自身的暂停标志（精确维度是否实际暂停还可能继承自更宽作用域）
 * @param limits 本次落盘的完整限额（暂停/恢复时为该精确维度当前生效值，仅便于回显）
 * @param timeMillis 操作时间
 * @param reason 操作说明
 */
public record GovernanceChangeResult(GovernanceScope scope, boolean paused, QuotaLimits limits,
                                     long timeMillis, String reason) {
}
