package com.github.highcumontoa.concurrenttaskschedulerjava.quota;

/**
 * 某个治理作用域的运行态快照。
 *
 * <p>作用域由 {@code scope} 标识：CALLER_GROUP（精确维度，callerId 与 group 均非空）、
 * CALLER（调用方整体，group 为 null，计数为该调用方下所有组的合计）、
 * GROUP（任务组整体，callerId 为 null，计数为该任务组下所有调用方的合计）。
 *
 * <p>计数（active/queued/startedInCurrentWindow）与生效配额、暂停状态在同一临界区内读取，
 * 保证与调度判定结果一致，不出现自相矛盾的视图。
 *
 * @param scope 作用域类型：CALLER_GROUP / CALLER / GROUP
 * @param callerId 调用方（GROUP 作用域为 null）
 * @param group 任务组（CALLER 作用域为 null）
 * @param maxConcurrency 当前生效的并发上限（&lt;=0 表示不限）
 * @param rateLimitPerSecond 当前生效的每秒启动上限（&lt;=0 表示不限）
 * @param maxQueued 当前生效的排队上限（&lt;=0 表示不限）
 * @param active 正在执行的任务数（整体作用域为合计）
 * @param queued 排队中的任务数（QUEUED + PENDING_RETRY；整体作用域为合计）
 * @param startedInCurrentWindow 本自然秒已启动的任务数（整体作用域为合计）
 * @param paused 暂停状态：CALLER_GROUP 为有效暂停（任一作用域暂停即为 true，与调度判定一致）；
 *               CALLER / GROUP 为该作用域自身的暂停标记
 * @param lastOperation 最近一次治理操作类型（ADJUST_QUOTA / PAUSE / RESUME），无则为 null
 * @param lastOperationAtEpochMillis 最近一次治理操作时间（epoch 毫秒），无则为 0
 * @param lastOperationDetail 最近一次治理操作内容（如改成什么值），无则为 null
 */
public record QuotaStatus(String scope, String callerId, String group,
                          int maxConcurrency, int rateLimitPerSecond, int maxQueued,
                          int active, int queued, int startedInCurrentWindow,
                          boolean paused,
                          String lastOperation, long lastOperationAtEpochMillis,
                          String lastOperationDetail) {
}
