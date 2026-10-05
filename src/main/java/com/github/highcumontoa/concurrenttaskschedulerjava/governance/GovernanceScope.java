package com.github.highcumontoa.concurrenttaskschedulerjava.governance;

/**
 * 治理作用域：一次配额调整或暂停/恢复操作作用的范围。
 *
 * <p>三种作用域（按优先级从高到低）：
 * <ul>
 *   <li>{@link Kind#CALLER_GROUP}：精确到某个调用方的某个任务组（callerId 与 group 均非空）；</li>
 *   <li>{@link Kind#CALLER}：整个调用方（callerId 非空、group 为 null），
 *       作用于该调用方下所有任务组；</li>
 *   <li>{@link Kind#GROUP}：整个任务组（group 非空、callerId 为 null），
 *       作用于该任务组下所有调用方。</li>
 * </ul>
 *
 * <p>callerId 与 group 不能同时为 null（缺少作用域信息）。
 * 空白字符串（非 null 但 isBlank）视为非法输入，由
 * {@link #of(String, String)} 抛出 {@link IllegalArgumentException}；
 * 只有 null 才表示“未指定该维度”。
 */
public record GovernanceScope(String callerId, String group) {

    public enum Kind {
        CALLER_GROUP, CALLER, GROUP
    }

    public GovernanceScope {
        if (callerId != null && callerId.isBlank()) {
            throw new IllegalArgumentException("callerId must not be blank");
        }
        if (group != null && group.isBlank()) {
            throw new IllegalArgumentException("group must not be blank");
        }
        if (callerId == null && group == null) {
            throw new IllegalArgumentException(
                    "callerId and group must not both be absent (missing governance scope)");
        }
    }

    public static GovernanceScope of(String callerId, String group) {
        return new GovernanceScope(callerId, group);
    }

    public Kind kind() {
        if (callerId != null && group != null) {
            return Kind.CALLER_GROUP;
        }
        return callerId != null ? Kind.CALLER : Kind.GROUP;
    }

    /** 该作用域是否覆盖运行时维度 (caller, group)。 */
    public boolean covers(String caller, String grp) {
        return (callerId == null || callerId.equals(caller))
                && (group == null || group.equals(grp));
    }

    @Override
    public String toString() {
        return switch (kind()) {
            case CALLER_GROUP -> callerId + ":" + group;
            case CALLER -> callerId + ":*";
            case GROUP -> "*:" + group;
        };
    }
}
