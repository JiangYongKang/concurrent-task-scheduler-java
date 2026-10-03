package com.github.highcumontoa.concurrenttaskschedulerjava.web.dto;

/**
 * 运行期配额调整请求（PATCH 语义：字段为 null 表示该项不改）。
 * 三项均为 0 表示不限制；负数非法，整次请求拒绝且原值不变。
 */
public class QuotaAdjustmentRequest {

    private Integer maxConcurrency;
    private Integer rateLimitPerSecond;
    private Integer maxQueued;
    private String reason;

    public Integer getMaxConcurrency() { return maxConcurrency; }
    public void setMaxConcurrency(Integer maxConcurrency) { this.maxConcurrency = maxConcurrency; }
    public Integer getRateLimitPerSecond() { return rateLimitPerSecond; }
    public void setRateLimitPerSecond(Integer rateLimitPerSecond) { this.rateLimitPerSecond = rateLimitPerSecond; }
    public Integer getMaxQueued() { return maxQueued; }
    public void setMaxQueued(Integer maxQueued) { this.maxQueued = maxQueued; }
    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }
}
