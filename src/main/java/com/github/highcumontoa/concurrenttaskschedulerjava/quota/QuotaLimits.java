package com.github.highcumontoa.concurrenttaskschedulerjava.quota;

/**
 * 单个配额维度的限制。
 *
 * @param maxConcurrency 最大并发执行数
 * @param rateLimitPerSecond 每秒最多启动执行数（令牌/启动速率限制），&lt;=0 表示不限
 * @param maxQueued 最多排队任务数（超出则拒绝），&lt;=0 表示不限
 */
public record QuotaLimits(int maxConcurrency, int rateLimitPerSecond, int maxQueued) {
}
