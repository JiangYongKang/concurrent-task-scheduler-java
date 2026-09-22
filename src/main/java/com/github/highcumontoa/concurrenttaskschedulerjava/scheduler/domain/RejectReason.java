package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain;

/** 配额不足导致拒绝时的确定性原因。 */
public enum RejectReason {
    CALLER_QUEUE_FULL,
    GLOBAL_QUEUE_FULL
}
