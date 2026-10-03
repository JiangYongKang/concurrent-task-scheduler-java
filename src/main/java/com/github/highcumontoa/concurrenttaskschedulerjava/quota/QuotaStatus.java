package com.github.highcumontoa.concurrenttaskschedulerjava.quota;

/**
 * 某个配额维度（caller + group）的运行态快照。
 *
 * <p>计数（active/queued/startedInCurrentWindow）与生效配额、暂停状态在同一临界区内读取，
 * 保证与调度判定结果一致，不出现自相矛盾的视图。
 *
 * @param callerId 调用方
 * @param group 任务组
 * @param maxConcurrency 当前生效的并发上限（&lt;=0 表示不限）
 * @param rateLimitPerSecond 当前生效的每秒启动上限（&lt;=0 表示不限）
 * @param maxQueued 当前生效的排队上限（&lt;=0 表示不限）
 * @param active 正在执行的任务数
 * @param queued 排队中的任务数（QUEUED + PENDING_RETRY）
 * @param startedInCurrentWindow 本自然秒已启动的任务数
 * @param paused 是否处于暂停派发状态
 * @param lastOperation 最近一次治理操作类型（ADJUST_QUOTA / PAUSE / RESUME），无则为 null
 * @param lastOperationAtEpochMillis 最近一次治理操作时间（epoch 毫秒），无则为 0
 * @param lastOperationDetail 最近一次治理操作内容（如改成什么值），无则为 null
 */
public record QuotaStatus(String callerId, String group,
                          int maxConcurrency, int rateLimitPerSecond, int maxQueued,
                          int active, int queued, int startedInCurrentWindow,
                          boolean paused,
                          String lastOperation, long lastOperationAtEpochMillis,
                          String lastOperationDetail) {
}
