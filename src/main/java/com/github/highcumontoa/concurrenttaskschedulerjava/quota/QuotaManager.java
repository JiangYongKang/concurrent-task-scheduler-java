package com.github.highcumontoa.concurrenttaskschedulerjava.quota;

import java.util.Map;
import java.util.Set;
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
 * <p>运行期治理支持三级作用域（见 {@link QuotaScope}）：
 * 精确(caller+group) &gt; 调用方级(caller) &gt; 任务组级(group) &gt; 静态配置 &gt; 默认值。
 * 暂停状态取所有匹配作用域的并集：任一匹配作用域被暂停，该维度即暂停派发。
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
    /** 静态配置（application.properties）的精确维度覆盖，优先级低于运行期覆盖。 */
    private final Map<QuotaKey, QuotaLimits> staticOverrides = new ConcurrentHashMap<>();
    /** 运行期配额覆盖，按作用域存放，三级作用域互不串扰。 */
    private final Map<QuotaScope, QuotaLimits> runtimeOverrides = new ConcurrentHashMap<>();
    /** 被暂停的作用域集合；某维度任一匹配作用域在集合中即视为暂停。 */
    private final Set<QuotaScope> pausedScopes = ConcurrentHashMap.newKeySet();

    public QuotaManager() {
    }

    private State state(QuotaKey key) {
        return states.computeIfAbsent(key, k -> new State());
    }

    /** 设置默认限制（未单独覆盖的维度使用）。 */
    public void setDefaultLimits(QuotaLimits limits) {
        if (limits == null) {
            throw new IllegalArgumentException("default limits must not be null");
        }
        this.defaultLimits = limits;
    }

    /** 静态配置覆盖（精确维度）；运行期调整优先于它。 */
    public void setLimits(QuotaKey key, QuotaLimits limits) {
        if (key == null || limits == null) {
            throw new IllegalArgumentException("key and limits must not be null");
        }
        staticOverrides.put(key, limits);
    }

    /**
     * 某维度当前生效的限制。
     * 生效顺序：运行期精确 &gt; 运行期调用方级 &gt; 运行期任务组级 &gt; 静态配置 &gt; 默认。
     */
    public QuotaLimits limits(QuotaKey key) {
        QuotaLimits exact = runtimeOverrides.get(QuotaScope.exact(key.callerId(), key.group()));
        if (exact != null) {
            return exact;
        }
        QuotaLimits caller = runtimeOverrides.get(QuotaScope.caller(key.callerId()));
        if (caller != null) {
            return caller;
        }
        QuotaLimits group = runtimeOverrides.get(QuotaScope.group(key.group()));
        if (group != null) {
            return group;
        }
        QuotaLimits stat = staticOverrides.get(key);
        return stat != null ? stat : defaultLimits;
    }

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

    // -------------------------------------------------- 作用域聚合视图（状态查询用）

    /** 某作用域下全部匹配维度的执行中任务数合计。 */
    public int activeCount(QuotaScope scope) {
        int sum = 0;
        for (Map.Entry<QuotaKey, State> e : states.entrySet()) {
            if (scope.matches(e.getKey())) {
                synchronized (e.getValue()) {
                    sum += e.getValue().active;
                }
            }
        }
        return sum;
    }

    /** 某作用域下全部匹配维度的排队任务数合计。 */
    public int queuedCount(QuotaScope scope) {
        int sum = 0;
        for (Map.Entry<QuotaKey, State> e : states.entrySet()) {
            if (scope.matches(e.getKey())) {
                synchronized (e.getValue()) {
                    sum += e.getValue().queued;
                }
            }
        }
        return sum;
    }

    /** 某作用域下全部匹配维度本秒窗口已启动数合计。 */
    public int startedInCurrentWindow(QuotaScope scope) {
        long epochSecond = System.currentTimeMillis() / 1000;
        int sum = 0;
        for (Map.Entry<QuotaKey, State> e : states.entrySet()) {
            if (scope.matches(e.getKey())) {
                synchronized (e.getValue()) {
                    rollWindowIfNeeded(e.getValue(), epochSecond);
                    sum += e.getValue().startedInWindow;
                }
            }
        }
        return sum;
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

    // ---------------------------------------------------------------- 运行期治理

    /**
     * 运行期调整某作用域的三项配额，立即参与后续调度判定。
     * 不合法的值（null / 任一项为负）抛出 {@link IllegalArgumentException}，
     * 原有生效值保持不变。
     *
     * <p>作用域之间互不串扰：精确作用域的调整只影响该 (caller, group)，
     * 不会改动同一调用方下其它组或同一任务组下其它调用方的覆盖。
     *
     * <p>调小并发上限不打断已在执行的任务（active 计数不变），
     * 只是后续 {@link #tryAcquireActive} 按新上限判定；调大后由调度器触发派发，
     * 积压任务按原有队头阻塞 FIFO 顺序放出。
     */
    public void adjustLimits(QuotaScope scope, QuotaLimits limits) {
        validate(scope, limits);
        runtimeOverrides.put(scope, limits);
    }

    /** 兼容入口：精确维度的运行期调整。 */
    public void adjustLimits(QuotaKey key, QuotaLimits limits) {
        adjustLimits(QuotaScope.exact(key.callerId(), key.group()), limits);
    }

    private static void validate(QuotaScope scope, QuotaLimits limits) {
        if (scope == null || limits == null) {
            throw new IllegalArgumentException("scope and limits must not be null");
        }
        if (limits.maxConcurrency() < 0 || limits.rateLimitPerSecond() < 0
                || limits.maxQueued() < 0) {
            throw new IllegalArgumentException(
                    "quota values must be >= 0 (0 means unlimited): " + limits);
        }
    }

    /** 暂停某作用域派发：新任务照常入队，执行中任务自然跑完。 */
    public void pause(QuotaScope scope) {
        if (scope == null) {
            throw new IllegalArgumentException("scope must not be null");
        }
        pausedScopes.add(scope);
    }

    /** 兼容入口：暂停精确维度。 */
    public void pause(QuotaKey key) {
        pause(QuotaScope.exact(key.callerId(), key.group()));
    }

    /** 恢复某作用域派发（只清除该作用域自身的暂停标记，不影响其它作用域）。 */
    public void resume(QuotaScope scope) {
        if (scope == null) {
            throw new IllegalArgumentException("scope must not be null");
        }
        pausedScopes.remove(scope);
    }

    /** 兼容入口：恢复精确维度。 */
    public void resume(QuotaKey key) {
        resume(QuotaScope.exact(key.callerId(), key.group()));
    }

    /** 某维度是否处于暂停：任一匹配作用域（精确/调用方级/任务组级）被暂停即为暂停。 */
    public boolean isPaused(QuotaKey key) {
        return pausedScopes.contains(QuotaScope.exact(key.callerId(), key.group()))
                || pausedScopes.contains(QuotaScope.caller(key.callerId()))
                || pausedScopes.contains(QuotaScope.group(key.group()));
    }

    /** 某作用域自身是否被暂停（不展开匹配，用于作用域级状态查询）。 */
    public boolean isScopePaused(QuotaScope scope) {
        return pausedScopes.contains(scope);
    }

    /** 当前已知（有计量状态或有配额覆盖）的全部精确维度。 */
    public Set<QuotaKey> knownKeys() {
        Set<QuotaKey> keys = new java.util.LinkedHashSet<>(states.keySet());
        keys.addAll(staticOverrides.keySet());
        for (QuotaScope scope : runtimeOverrides.keySet()) {
            if (scope.isExact()) {
                keys.add(QuotaKey.of(scope.callerId(), scope.group()));
            }
        }
        return keys;
    }

    /** 当前有运行期覆盖或暂停标记的全部作用域（含调用方级/任务组级）。 */
    public Set<QuotaScope> knownScopes() {
        Set<QuotaScope> scopes = new java.util.LinkedHashSet<>(runtimeOverrides.keySet());
        scopes.addAll(pausedScopes);
        return scopes;
    }

    /** 当前默认限制（未覆盖的作用域/维度回落到它）。 */
    public QuotaLimits defaultLimits() {
        return defaultLimits;
    }

    /** 某作用域自身的运行期配额覆盖（无则 null，用于作用域级状态查询）。 */
    public QuotaLimits scopeOverride(QuotaScope scope) {
        return runtimeOverrides.get(scope);
    }
}
