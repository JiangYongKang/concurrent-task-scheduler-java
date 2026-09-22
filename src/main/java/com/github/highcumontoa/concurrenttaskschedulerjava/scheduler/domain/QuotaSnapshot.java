package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain;

/** 某个作用域(全局/调用方)当前配额占用的只读视图。 */
public class QuotaSnapshot {

    private final int maxConcurrency;
    private final int running;
    private final int maxQueued;
    private final int queued;
    private final int rateLimitPerSecond;
    private final double availableTokens;

    public QuotaSnapshot(int maxConcurrency, int running, int maxQueued, int queued,
                         int rateLimitPerSecond, double availableTokens) {
        this.maxConcurrency = maxConcurrency;
        this.running = running;
        this.maxQueued = maxQueued;
        this.queued = queued;
        this.rateLimitPerSecond = rateLimitPerSecond;
        this.availableTokens = availableTokens;
    }

    public int getMaxConcurrency() { return maxConcurrency; }
    public int getRunning() { return running; }
    public int getMaxQueued() { return maxQueued; }
    public int getQueued() { return queued; }
    public int getRateLimitPerSecond() { return rateLimitPerSecond; }
    public double getAvailableTokens() { return availableTokens; }
}
