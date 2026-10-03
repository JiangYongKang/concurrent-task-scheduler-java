package com.github.highcumontoa.concurrenttaskschedulerjava.quota;

import com.github.highcumontoa.concurrenttaskschedulerjava.governance.GovernanceScope;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 配额管理器：按 {@link QuotaKey}（caller + group）维护
 * 排队长度、并发占用与固定 1 秒窗口启动计数。
 *
 * <p>设计约束：所有“判定 + 占用”必须与调度器在同一把锁内串行调用，
 * {@link #tryAcquireActive} 内部同步仅为防御性保证，判定与占用在本方法内原子完成，
 * 杜绝 TOCTOU 越限执行。
 *
 * <p>速率限制采用固定窗口（每个自然秒最多启动 rateLimitPerSecond 个），
 * 已消耗的窗口额度在执行结束时不回补，避免取消/快速失败刷启动速率。
 * 进程重启后由 {@link #reinitialize} 按 WAL 恢复占用，防止配额泄漏或超发。
 *
 * <p>运行期治理：生效配额支持三级作用域覆盖，优先级从高到低为
 * <b>运行期精确(caller,group) &gt; 静态配置精确 &gt; 运行期调用方 &gt; 运行期任务组
 * &gt; 默认</b>；暂停同理，三个作用域任一暂停即视为该精确维度暂停。
 * 运行期调整立即参与后续判定，不中断、不回退已在执行的任务。
 */
public class QuotaManager {

    /** 单个维度的可变计量状态。 */
    private static final class State {
        int queued;
        int active;
        long windowEpochSecond = Long.MIN_VALUE;
        int startedInWindow;
    }

    private final Map<QuotaKey, State> states = new ConcurrentHashMap<>();
    private volatile QuotaLimits defaultLimits = new QuotaLimits(1, 0, 0);
    /** 启动期静态配置覆盖（不可在运行期修改）。 */
    private final Map<QuotaKey, QuotaLimits> overrides = new ConcurrentHashMap<>();

    // ---- 运行期治理状态（所有治理映射都在 governance 锁下读写）----
    private final Object governanceLock = new Object();
    private final Map<String, QuotaLimits> runtimeLimits = new ConcurrentHashMap<>();
    private final Map<String, Boolean> pausedScopes = new ConcurrentHashMap<>();
    private final Map<String, Long> changeTimes = new ConcurrentHashMap<>();
    private final Map<String, String> changeDescriptions = new ConcurrentHashMap<>();

    public QuotaManager() {
    }

    private State state(QuotaKey key) {
        return states.computeIfAbsent(key, k -> new State());
    }

    // ---------------------------------------------------------------- 静态/默认配置

    /** 设置默认限制（未单独覆盖的维度使用）。 */
    public void setDefaultLimits(QuotaLimits limits) {
        if (limits == null) {
            throw new IllegalArgumentException("default limits must not be null");
        }
        this.defaultLimits = limits;
    }

    /** 启动期静态配置覆盖（配置文件来源，运行期不经过这里修改）。 */
    public void setLimits(QuotaKey key, QuotaLimits limits) {
        if (key == null || limits == null) {
            throw new IllegalArgumentException("key and limits must not be null");
        }
        overrides.put(key, limits);
    }

    // ---------------------------------------------------------------- 生效配额解析

    public QuotaLimits limits(QuotaKey key) {
        return resolveLimits(key.callerId(), key.group());
    }

    /**
     * 按优先级解析生效配额：运行期精确 &gt; 静态精确 &gt; 运行期 caller &gt; 运行期 group &gt; 默认。
     * 三项字段各自独立解析，保证 PATCH 只改某一项时其余项仍取较低优先级的值。
     */
    public QuotaLimits resolveLimits(String callerId, String group) {
        QuotaLimits def = defaultLimits;
        int concurrency = def.maxConcurrency();
        int rate = def.rateLimitPerSecond();
        int queued = def.maxQueued();
        synchronized (governanceLock) {
            QuotaLimits g = runtimeLimits.get(scopeTag(GovernanceScope.Kind.GROUP, null, group));
            if (g != null) {
                concurrency = g.maxConcurrency();
                rate = g.rateLimitPerSecond();
                queued = g.maxQueued();
            }
            QuotaLimits c = runtimeLimits.get(
                    scopeTag(GovernanceScope.Kind.CALLER, callerId, null));
            if (c != null) {
                concurrency = c.maxConcurrency();
                rate = c.rateLimitPerSecond();
                queued = c.maxQueued();
            }
        }
        QuotaLimits exactStatic = overrides.get(QuotaKey.of(callerId, group));
        if (exactStatic != null) {
            concurrency = exactStatic.maxConcurrency();
            rate = exactStatic.rateLimitPerSecond();
            queued = exactStatic.maxQueued();
        }
        synchronized (governanceLock) {
            QuotaLimits exactRuntime = runtimeLimits.get(
                    scopeTag(GovernanceScope.Kind.EXACT, callerId, group));
            if (exactRuntime != null) {
                concurrency = exactRuntime.maxConcurrency();
                rate = exactRuntime.rateLimitPerSecond();
                queued = exactRuntime.maxQueued();
            }
        }
        return new QuotaLimits(concurrency, rate, queued);
    }

    // ---------------------------------------------------------------- 运行期治理写入

    /**
     * 运行期覆盖某维度的完整三项配额。非法值（负数）抛 IllegalArgumentException，
     * 不修改任何现有状态（调用方保证先校验再调用）。
     */
    public void applyRuntimeLimits(GovernanceScope scope, QuotaLimits limits) {
        validate(limits);
        synchronized (governanceLock) {
            runtimeLimits.put(tag(scope), limits);
        }
    }

    /** 设置某维度暂停状态。 */
    public void setPaused(GovernanceScope scope, boolean paused) {
        synchronized (governanceLock) {
            if (paused) {
                pausedScopes.put(tag(scope), Boolean.TRUE);
            } else {
                pausedScopes.remove(tag(scope));
            }
        }
    }

    /** 记录最近一次治理操作（重启回放时同样写入，恢复“最近操作”可观测信息）。 */
    public void recordLastChange(GovernanceScope scope, long timeMillis, String description) {
        synchronized (governanceLock) {
            changeTimes.put(tag(scope), timeMillis);
            if (description == null) {
                changeDescriptions.remove(tag(scope));
            } else {
                changeDescriptions.put(tag(scope), description);
            }
        }
    }

    private static void validate(QuotaLimits limits) {
        if (limits == null) {
            throw new IllegalArgumentException("limits must not be null");
        }
        if (limits.maxConcurrency() < 0 || limits.rateLimitPerSecond() < 0
                || limits.maxQueued() < 0) {
            throw new IllegalArgumentException(
                    "quota values must be >= 0 (0 means unlimited): " + limits);
        }
    }

    /** 精确维度是否暂停：精确 / 调用方 / 任务组任一作用域暂停即暂停。 */
    public boolean isPaused(QuotaKey key) {
        return isPaused(key.callerId(), key.group());
    }

    public boolean isPaused(String callerId, String group) {
        synchronized (governanceLock) {
            return pausedScopes.containsKey(
                            scopeTag(GovernanceScope.Kind.EXACT, callerId, group))
                    || pausedScopes.containsKey(
                            scopeTag(GovernanceScope.Kind.CALLER, callerId, null))
                    || pausedScopes.containsKey(
                            scopeTag(GovernanceScope.Kind.GROUP, null, group));
        }
    }

    /** 某作用域自身的暂停标志（不含更宽作用域继承），用于变更结果回显。 */
    public boolean scopePaused(GovernanceScope scope) {
        synchronized (governanceLock) {
            return pausedScopes.containsKey(tag(scope));
        }
    }

    /** 某作用域自身已保存的运行期限额（无则 null），用于 PATCH 合并基准与回放。 */
    public QuotaLimits runtimeScopeLimits(GovernanceScope scope) {
        synchronized (governanceLock) {
            return runtimeLimits.get(tag(scope));
        }
    }

    /** 某作用域做 PATCH 合并时的基准值：已有运行期值，否则取默认配置。 */
    public QuotaLimits scopeBaselineLimits(GovernanceScope scope) {
        QuotaLimits existing = runtimeScopeLimits(scope);
        return existing != null ? existing : defaultLimits;
    }

    /** 最近一次治理操作时间（毫秒），无记录返回 -1；取三个作用域中的最新者。 */
    public long lastChangeTimeMillis(String callerId, String group) {
        synchronized (governanceLock) {
            long t = -1L;
            t = Math.max(t, time(scopeTag(GovernanceScope.Kind.EXACT, callerId, group)));
            t = Math.max(t, time(scopeTag(GovernanceScope.Kind.CALLER, callerId, null)));
            t = Math.max(t, time(scopeTag(GovernanceScope.Kind.GROUP, null, group)));
            return t;
        }
    }

    /** 最近一次治理操作描述：精确 &gt; 调用方 &gt; 任务组，返回命中的最新时间作用域描述。 */
    public String lastChangeDescription(String callerId, String group) {
        synchronized (governanceLock) {
            String exact = scopeTag(GovernanceScope.Kind.EXACT, callerId, group);
            String caller = scopeTag(GovernanceScope.Kind.CALLER, callerId, null);
            String grp = scopeTag(GovernanceScope.Kind.GROUP, null, group);
            String best = null;
            long bestTime = Long.MIN_VALUE;
            for (String t : new String[]{exact, caller, grp}) {
                Long tm = changeTimes.get(t);
                if (tm != null && tm >= bestTime) {
                    bestTime = tm;
                    best = changeDescriptions.get(t);
                }
            }
            return best;
        }
    }

    private long time(String tag) {
        Long v = changeTimes.get(tag);
        return v == null ? -1L : v;
    }

    private static String tag(GovernanceScope scope) {
        return scopeTag(scope.kind(), scope.callerId(), scope.group());
    }

    private static String scopeTag(GovernanceScope.Kind kind, String callerId, String group) {
        return switch (kind) {
            case CALLER -> "C:" + callerId;
            case GROUP -> "G:" + group;
            case EXACT -> "E:" + callerId + "|" + group;
        };
    }

    // ---------------------------------------------------------------- 计量与判定

    public int activeCount(QuotaKey key) {
        synchronized (state(key)) {
            return state(key).active;
        }
    }

    public int queuedCount(QuotaKey key) {
        synchronized (state(key)) {
            return state(key).queued;
        }
    }

    /** 当前秒窗口内已启动数（测试/可观测使用）。 */
    public int startedInCurrentWindow(QuotaKey key) {
        State s = state(key);
        synchronized (s) {
            rollWindowIfNeeded(s, System.currentTimeMillis() / 1000);
            return s.startedInWindow;
        }
    }

    public boolean tryEnqueue(QuotaKey key) {
        State s = state(key);
        synchronized (s) {
            int maxQueued = limits(key).maxQueued();
            if (maxQueued > 0 && s.queued >= maxQueued) {
                return false;
            }
            s.queued++;
            return true;
        }
    }

    public void releaseQueued(QuotaKey key) {
        State s = state(key);
        synchronized (s) {
            if (s.queued > 0) {
                s.queued--;
            }
        }
    }

    public boolean tryAcquireActive(QuotaKey key) {
        State s = state(key);
        synchronized (s) {
            QuotaLimits limits = limits(key);
            long epochSecond = System.currentTimeMillis() / 1000;
            rollWindowIfNeeded(s, epochSecond);
            if (limits.maxConcurrency() > 0 && s.active >= limits.maxConcurrency()) {
                return false;
            }
            if (limits.rateLimitPerSecond() > 0 && s.startedInWindow >= limits.rateLimitPerSecond()) {
                return false;
            }
            s.active++;
            s.startedInWindow++;
            return true;
        }
    }

    public void releaseActive(QuotaKey key) {
        State s = state(key);
        synchronized (s) {
            if (s.active > 0) {
                s.active--;
            }
        }
    }

    private void rollWindowIfNeeded(State s, long epochSecond) {
        if (s.windowEpochSecond != epochSecond) {
            s.windowEpochSecond = epochSecond;
            s.startedInWindow = 0;
        }
    }

    public void reinitialize(QuotaKey key, int queued, int active) {
        State s = state(key);
        synchronized (s) {
            s.queued = Math.max(0, queued);
            s.active = Math.max(0, active);
            // 恢复后保守处理当前秒窗口：按 active 数预占启动额度，
            // 防止重启瞬间在本秒内超额启动。
            long epochSecond = System.currentTimeMillis() / 1000;
            s.windowEpochSecond = epochSecond;
            s.startedInWindow = s.active;
        }
    }
}
