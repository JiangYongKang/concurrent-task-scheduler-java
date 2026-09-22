package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;

/** scheduler.* 配置项。 */
@ConfigurationProperties(prefix = "scheduler")
public class SchedulerProperties {

    private String dataDir = "data/tasks";
    private int globalMaxConcurrency = 8;
    private int globalMaxQueued = 200;
    private int globalRateLimitPerSecond = 20;
    private int defaultMaxConcurrency = 2;
    private int defaultMaxQueued = 50;
    private int defaultRateLimitPerSecond = 5;
    private long attemptTimeoutMillis = 30000;
    private int defaultMaxAttempts = 3;
    private long backoffBaseMillis = 200;
    private double backoffMultiplier = 2.0;
    private long maxBackoffMillis = 10000;
    private boolean retryRunningTasksOnRestart = false;
    private long tickMillis = 10;
    private Map<String, CallerQuota> callers = new LinkedHashMap<>();

    public static class CallerQuota {
        private Integer maxConcurrency;
        private Integer maxQueued;
        private Integer rateLimitPerSecond;
        private Integer maxAttempts;

        public Integer getMaxConcurrency() { return maxConcurrency; }
        public void setMaxConcurrency(Integer maxConcurrency) { this.maxConcurrency = maxConcurrency; }
        public Integer getMaxQueued() { return maxQueued; }
        public void setMaxQueued(Integer maxQueued) { this.maxQueued = maxQueued; }
        public Integer getRateLimitPerSecond() { return rateLimitPerSecond; }
        public void setRateLimitPerSecond(Integer rateLimitPerSecond) { this.rateLimitPerSecond = rateLimitPerSecond; }
        public Integer getMaxAttempts() { return maxAttempts; }
        public void setMaxAttempts(Integer maxAttempts) { this.maxAttempts = maxAttempts; }
    }

    public String getDataDir() { return dataDir; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }
    public int getGlobalMaxConcurrency() { return globalMaxConcurrency; }
    public void setGlobalMaxConcurrency(int globalMaxConcurrency) { this.globalMaxConcurrency = globalMaxConcurrency; }
    public int getGlobalMaxQueued() { return globalMaxQueued; }
    public void setGlobalMaxQueued(int globalMaxQueued) { this.globalMaxQueued = globalMaxQueued; }
    public int getGlobalRateLimitPerSecond() { return globalRateLimitPerSecond; }
    public void setGlobalRateLimitPerSecond(int globalRateLimitPerSecond) { this.globalRateLimitPerSecond = globalRateLimitPerSecond; }
    public int getDefaultMaxConcurrency() { return defaultMaxConcurrency; }
    public void setDefaultMaxConcurrency(int defaultMaxConcurrency) { this.defaultMaxConcurrency = defaultMaxConcurrency; }
    public int getDefaultMaxQueued() { return defaultMaxQueued; }
    public void setDefaultMaxQueued(int defaultMaxQueued) { this.defaultMaxQueued = defaultMaxQueued; }
    public int getDefaultRateLimitPerSecond() { return defaultRateLimitPerSecond; }
    public void setDefaultRateLimitPerSecond(int defaultRateLimitPerSecond) { this.defaultRateLimitPerSecond = defaultRateLimitPerSecond; }
    public long getAttemptTimeoutMillis() { return attemptTimeoutMillis; }
    public void setAttemptTimeoutMillis(long attemptTimeoutMillis) { this.attemptTimeoutMillis = attemptTimeoutMillis; }
    public int getDefaultMaxAttempts() { return defaultMaxAttempts; }
    public void setDefaultMaxAttempts(int defaultMaxAttempts) { this.defaultMaxAttempts = defaultMaxAttempts; }
    public long getBackoffBaseMillis() { return backoffBaseMillis; }
    public void setBackoffBaseMillis(long backoffBaseMillis) { this.backoffBaseMillis = backoffBaseMillis; }
    public double getBackoffMultiplier() { return backoffMultiplier; }
    public void setBackoffMultiplier(double backoffMultiplier) { this.backoffMultiplier = backoffMultiplier; }
    public long getMaxBackoffMillis() { return maxBackoffMillis; }
    public void setMaxBackoffMillis(long maxBackoffMillis) { this.maxBackoffMillis = maxBackoffMillis; }
    public boolean isRetryRunningTasksOnRestart() { return retryRunningTasksOnRestart; }
    public void setRetryRunningTasksOnRestart(boolean retryRunningTasksOnRestart) { this.retryRunningTasksOnRestart = retryRunningTasksOnRestart; }
    public long getTickMillis() { return tickMillis; }
    public void setTickMillis(long tickMillis) { this.tickMillis = tickMillis; }
    public Map<String, CallerQuota> getCallers() { return callers; }
    public void setCallers(Map<String, CallerQuota> callers) { this.callers = callers; }
}
