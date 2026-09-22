package com.github.highcumontoa.concurrenttaskschedulerjava.quota;

/** 配额维度：调用方 + 任务组。 */
public record QuotaKey(String callerId, String group) {

    public static QuotaKey of(String callerId, String group) {
        return new QuotaKey(callerId, group);
    }
}
