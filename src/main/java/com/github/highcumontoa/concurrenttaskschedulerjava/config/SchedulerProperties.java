package com.github.highcumontoa.concurrenttaskschedulerjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 调度器配置（前缀 task.scheduler）。
 */
@ConfigurationProperties(prefix = "task.scheduler")
public class SchedulerProperties {

    /** 全局执行线程池大小（所有任务类型共享的硬上限）。 */
    private int workerThreads = 4;
    /** 每个 (caller, group) 的默认并发上限。 */
    private int defaultMaxConcurrency = 2;
    /** 每个 (caller, group) 的默认每秒启动上限。 */
    private int defaultRateLimitPerSecond = 2;
    /** 每个 (caller, group) 的默认排队上限。 */
    private int defaultMaxQueued = 100;
    /** 默认执行超时毫秒（&lt;=0 不限）。 */
    private long defaultTimeoutMillis = 30_000;
    /** 默认最大重试次数（不含首次执行）。 */
    private int defaultMaxRetries = 2;
    /** 退避基数毫秒。 */
    private long backoffBaseMillis = 200;
    /** 退避乘数。 */
    private double backoffMultiplier = 2.0;
    /** 退避上限毫秒。 */
    private long backoffCapMillis = 10_000;
    /** 退避抖动比例 [0,1)。 */
    private double backoffJitter = 0.0;
    /** WAL 文件路径。 */
    private String walFile = "data/task-scheduler.wal";
    /** 每次写 WAL 是否 fsync（测试关盘可关闭）。 */
    private boolean walFsync = true;
    /** 按 caller 或 caller:group 覆盖配额。 */
    private Map<String, QuotaOverride> quotas = new LinkedHashMap<>();

    public static class QuotaOverride {
        private Integer maxConcurrency;
        private Integer rateLimitPerSecond;
        private Integer maxQueued;

        public Integer getMaxConcurrency() { return maxConcurrency; }
        public void setMaxConcurrency(Integer maxConcurrency) { this.maxConcurrency = maxConcurrency; }
        public Integer getRateLimitPerSecond() { return rateLimitPerSecond; }
        public void setRateLimitPerSecond(Integer rateLimitPerSecond) { this.rateLimitPerSecond = rateLimitPerSecond; }
        public Integer getMaxQueued() { return maxQueued; }
        public void setMaxQueued(Integer maxQueued) { this.maxQueued = maxQueued; }
    }

    public int getWorkerThreads() { return workerThreads; }
    public void setWorkerThreads(int workerThreads) { this.workerThreads = workerThreads; }
    public int getDefaultMaxConcurrency() { return defaultMaxConcurrency; }
    public void setDefaultMaxConcurrency(int v) { this.defaultMaxConcurrency = v; }
    public int getDefaultRateLimitPerSecond() { return defaultRateLimitPerSecond; }
    public void setDefaultRateLimitPerSecond(int v) { this.defaultRateLimitPerSecond = v; }
    public int getDefaultMaxQueued() { return defaultMaxQueued; }
    public void setDefaultMaxQueued(int v) { this.defaultMaxQueued = v; }
    public long getDefaultTimeoutMillis() { return defaultTimeoutMillis; }
    public void setDefaultTimeoutMillis(long v) { this.defaultTimeoutMillis = v; }
    public int getDefaultMaxRetries() { return defaultMaxRetries; }
    public void setDefaultMaxRetries(int v) { this.defaultMaxRetries = v; }
    public long getBackoffBaseMillis() { return backoffBaseMillis; }
    public void setBackoffBaseMillis(long v) { this.backoffBaseMillis = v; }
    public double getBackoffMultiplier() { return backoffMultiplier; }
    public void setBackoffMultiplier(double v) { this.backoffMultiplier = v; }
    public long getBackoffCapMillis() { return backoffCapMillis; }
    public void setBackoffCapMillis(long v) { this.backoffCapMillis = v; }
    public double getBackoffJitter() { return backoffJitter; }
    public void setBackoffJitter(double v) { this.backoffJitter = v; }
    public String getWalFile() { return walFile; }
    public void setWalFile(String walFile) { this.walFile = walFile; }
    public boolean isWalFsync() { return walFsync; }
    public void setWalFsync(boolean walFsync) { this.walFsync = walFsync; }
    public Map<String, QuotaOverride> getQuotas() { return quotas; }
    public void setQuotas(Map<String, QuotaOverride> quotas) { this.quotas = quotas; }
}
