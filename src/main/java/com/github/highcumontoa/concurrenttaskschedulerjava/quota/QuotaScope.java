package com.github.highcumontoa.concurrenttaskschedulerjava.quota;

/**
 * 治理作用域：配额覆盖与暂停状态可以挂在三个层级上。
 *
 * <ul>
 *   <li>{@code exact(caller, group)}：精确到某个调用方的某个任务组；</li>
 *   <li>{@code caller(caller)}：整个调用方（其下全部任务组）；</li>
 *   <li>{@code group(group)}：整个任务组（其下全部调用方）。</li>
 * </ul>
 *
 * <p>callerId / group 恰有一个可以为 null（null 表示该维度不限定），
 * 不允许两者都为 null（缺少作用域信息的请求必须被拒绝）。
 *
 * <p>多个作用域同时存在时的生效优先级（高 → 低）：
 * 精确(caller+group) &gt; 调用方级(caller) &gt; 任务组级(group)。
 * 各作用域的覆盖与暂停状态相互独立，精确作用域的调整不会改动
 * 同一调用方下其它组、或同一任务组下其它调用方的状态。
 */
public record QuotaScope(String callerId, String group) {

    public QuotaScope {
        boolean hasCaller = callerId != null && !callerId.isBlank();
        boolean hasGroup = group != null && !group.isBlank();
        if (!hasCaller && !hasGroup) {
            throw new IllegalArgumentException(
                    "scope requires at least one of callerId/group");
        }
    }

    /** 精确作用域：某个调用方的某个任务组。 */
    public static QuotaScope exact(String callerId, String group) {
        return new QuotaScope(callerId, group);
    }

    /** 调用方级作用域：该调用方下全部任务组。 */
    public static QuotaScope caller(String callerId) {
        return new QuotaScope(callerId, null);
    }

    /** 任务组级作用域：该任务组下全部调用方。 */
    public static QuotaScope group(String group) {
        return new QuotaScope(null, group);
    }

    public boolean isExact() {
        return callerId != null && group != null;
    }

    public boolean isCallerLevel() {
        return callerId != null && group == null;
    }

    public boolean isGroupLevel() {
        return callerId == null && group != null;
    }

    /** 该作用域是否覆盖某个具体配额维度。 */
    public boolean matches(QuotaKey key) {
        if (callerId != null && !callerId.equals(key.callerId())) {
            return false;
        }
        return group == null || group.equals(key.group());
    }

    /** 人类可读标识，用于日志与治理记录。 */
    public String describe() {
        if (isExact()) {
            return callerId + ":" + group;
        }
        return isCallerLevel() ? callerId + ":*" : "*:" + group;
    }
}
