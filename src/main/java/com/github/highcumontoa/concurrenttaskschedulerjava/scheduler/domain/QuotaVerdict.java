package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain;

/** 一次提交/派发的配额判定结果。 */
public enum QuotaVerdict {
    START,
    ENQUEUE,
    REJECT
}
