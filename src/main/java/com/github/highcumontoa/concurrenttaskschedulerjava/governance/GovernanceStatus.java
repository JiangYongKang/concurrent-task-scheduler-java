package com.github.highcumontoa.concurrenttaskschedulerjava.governance;

import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaLimits;

/**
 * 某个精确配额维度（caller + group）当前运行态快照。
 *
 * <p>所有字段均在调度器锁内一次性取齐，保证与真实调度判定自洽。
 *
 * @param callerId 调用方
 * @param group 任务组
 * @param active 正在执行数
 * @param queued 排队数（QUEUED + PENDING_RETRY）
 * @param startedInCurrentSecond 当前自然秒已启动数
 * @param limits 当前生效的三项配额（跨维度解析后）
 * @param paused 是否处于暂停派发状态
 * @param lastChangeTimeMillis 最近一次调整/暂停操作时间（无则 -1）
 * @param lastChangeDescription 最近一次操作的可读描述（无则 null）
 */
public record GovernanceStatus(String callerId, String group, int active, int queued,
                               int startedInCurrentSecond, QuotaLimits limits, boolean paused,
                               long lastChangeTimeMillis, String lastChangeDescription) {
}
