package com.github.highcumontoa.concurrenttaskschedulerjava.governance;

/**
 * 运行期治理操作的作用维度。
 *
 * <p>支持三种精确语义：
 * <ul>
 *   <li>{@code CALLER}：按调用方（该 caller 的所有任务组）；</li>
 *   <li>{@code GROUP}：按任务组（所有调用方的该 group）；</li>
 *   <li>{@code EXACT}：按调用方 + 任务组精确定位。</li>
 * </ul>
 */
public record GovernanceScope(Kind kind, String callerId, String group) {

    public enum Kind { CALLER, GROUP, EXACT }

    /** 按调用方维度（覆盖其全部任务组）。 */
    public static GovernanceScope caller(String callerId) {
        return new GovernanceScope(Kind.CALLER, callerId, null);
    }

    /** 按任务组维度（覆盖全部调用方的该组）。 */
    public static GovernanceScope group(String group) {
        return new GovernanceScope(Kind.GROUP, null, group);
    }

    /** 按调用方 + 任务组精确维度。 */
    public static GovernanceScope exact(String callerId, String group) {
        return new GovernanceScope(Kind.EXACT, callerId, group);
    }
}
