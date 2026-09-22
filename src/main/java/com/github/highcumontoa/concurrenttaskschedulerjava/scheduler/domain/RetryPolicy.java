package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain;

/**
 * 重试策略：最大尝试次数与指数退避。
 * 退避公式：delay = min(base * multiplier^(attempt-1), maxBackoff)。
 */
public class RetryPolicy {

    private final int maxAttempts;
    private final long baseMillis;
    private final double multiplier;
    private final long maxBackoffMillis;

    public RetryPolicy(int maxAttempts, long baseMillis, double multiplier, long maxBackoffMillis) {
        this.maxAttempts = maxAttempts;
        this.baseMillis = baseMillis;
        this.multiplier = multiplier;
        this.maxBackoffMillis = maxBackoffMillis;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public long getBaseMillis() {
        return baseMillis;
    }

    public double getMultiplier() {
        return multiplier;
    }

    public long getMaxBackoffMillis() {
        return maxBackoffMillis;
    }

    /**
     * 计算第 {@code failedAttempt} 次尝试失败后的退避时长。
     *
     * @param failedAttempt 已失败的尝试序号（从 1 开始）
     */
    public long backoffMillis(int failedAttempt) {
        double delay = baseMillis * Math.pow(multiplier, Math.max(0, failedAttempt - 1));
        long millis = (long) Math.min(delay, (double) maxBackoffMillis);
        return Math.max(0, millis);
    }
}
