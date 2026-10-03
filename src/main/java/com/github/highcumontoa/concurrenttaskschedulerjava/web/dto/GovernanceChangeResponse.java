package com.github.highcumontoa.concurrenttaskschedulerjava.web.dto;

import com.github.highcumontoa.concurrenttaskschedulerjava.governance.GovernanceChangeResult;
import com.github.highcumontoa.concurrenttaskschedulerjava.governance.GovernanceScope;
import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaLimits;

/**
 * 治理变更（配额调整 / 暂停 / 恢复）响应。
 */
public record GovernanceChangeResponse(String scopeKind, String callerId, String group,
                                       boolean paused, Integer maxConcurrency,
                                       Integer rateLimitPerSecond, Integer maxQueued,
                                       long timeMillis, String reason) {

    public static GovernanceChangeResponse from(GovernanceChangeResult r) {
        GovernanceScope s = r.scope();
        QuotaLimits l = r.limits();
        return new GovernanceChangeResponse(s.kind().name(), s.callerId(), s.group(),
                r.paused(),
                l == null ? null : l.maxConcurrency(),
                l == null ? null : l.rateLimitPerSecond(),
                l == null ? null : l.maxQueued(),
                r.timeMillis(), r.reason());
    }
}
