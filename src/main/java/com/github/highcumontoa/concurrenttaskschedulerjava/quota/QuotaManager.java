package com.github.highcumontoa.concurrenttaskschedulerjava.quota;

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
 * <p>运行期治理支持三种作用域：精确维度(caller+group)、调用方整体、任务组整体。
 * 配额解析优先级：精确 &gt; 调用方 &gt; 任务组 &gt; 默认值；
 * 暂停为叠加语义：任一作用域暂停即暂停该维度派发。
 */
public class QuotaManager {

    /** 单个维度的可变计量状态。 */
    private static final class State {
        int queued;
        int active;
        long windowEpochSecond = Long.MIN_VALUE;
        int startedInWindow;
        /** 暂停派发：新任务照常排队，但不再被启动；执行中任务不受影响。 */
        boolean paused;
    }

    private final Map<QuotaKey, State> states = new ConcurrentHashMap<>();
    private volatile QuotaLimits defaultLimits = new QuotaLimits(1, 0, 0);
    private final Map<QuotaKey, QuotaLimits> overrides = new ConcurrentHashMap<>();
    /** 按调用方整体的配额覆盖（作用于该 caller 下所有组的维度）。 */
    private final Map<String, QuotaLimits> callerOverrides = new ConcurrentHashMap<>();
    /** 按任务组整体的配额覆盖（作用于该 group 下所有 caller 的维度）。 */
    private final Map<String, QuotaLimits> groupOverrides = new ConcurrentHashMap<>();
    /** 按调用方整体的暂停标记。 */
    private final java.util.Set<String> pausedCallers =
            java.util.concurrent.ConcurrentHashMap.newKeySet();
    /** 按任务组整体的暂停标记。 */
    private final java.util.Set<String> pausedGroups =
            java.util.concurrent.ConcurrentHashMap.newKeySet();

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

    public void setLimits(QuotaKey key, QuotaLimits limits) {
        if (key == null || limits == null) {
            throw new IllegalArgumentException("key and limits must not be null");
        }
        overrides.put(key, limits);
    }

    /**
     * 某运行维度的生效配额，按作用域优先级解析：
     * 精确维度(caller+group)覆盖 &gt; 调用方整体覆盖 &gt; 任务组整体覆盖 &gt; 默认值。
     */
    public QuotaLimits limits(QuotaKey key) {
        QuotaLimits override = overrides.get(key);
        if (override != null) {
            return override;
        }
        QuotaLimits callerOverride = callerOverrides.get(key.callerId());
        if (callerOverride != null) {
            return callerOverride;
        }
        QuotaLimits groupOverride = groupOverrides.get(key.group());
        return groupOverride != null ? groupOverride : defaultLimits;
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
     * 运行期调整某维度的三项配额，立即参与后续调度判定。
     * 不合法的值（null / 任一项为负）抛出 {@link IllegalArgumentException}，
     * 原有生效值保持不变。
     *
     * <p>调小并发上限不打断已在执行的任务（active 计数不变），
     * 只是后续 {@link #tryAcquireActive} 按新上限判定；调大后由调度器触发派发，
     * 积压任务按原有队头阻塞 FIFO 顺序放出。
     */
    public void adjustLimits(QuotaKey key, QuotaLimits limits) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        validateLimits(limits);
        overrides.put(key, limits);
    }

    /** 运行期调整某个调用方整体的三项配额：作用于该 caller 下所有组的维度。 */
    public void adjustCallerLimits(String callerId, QuotaLimits limits) {
        if (callerId == null || callerId.isBlank()) {
            throw new IllegalArgumentException("callerId must not be blank");
        }
        validateLimits(limits);
        callerOverrides.put(callerId, limits);
    }

    /** 运行期调整某个任务组整体的三项配额：作用于该 group 下所有 caller 的维度。 */
    public void adjustGroupLimits(String group, QuotaLimits limits) {
        if (group == null || group.isBlank()) {
            throw new IllegalArgumentException("group must not be blank");
        }
        validateLimits(limits);
        groupOverrides.put(group, limits);
    }

    private static void validateLimits(QuotaLimits limits) {
        if (limits == null) {
            throw new IllegalArgumentException("limits must not be null");
        }
        if (limits.maxConcurrency() < 0 || limits.rateLimitPerSecond() < 0
                || limits.maxQueued() < 0) {
            throw new IllegalArgumentException(
                    "quota values must be >= 0 (0 means unlimited): " + limits);
        }
    }

    /** 暂停某维度派发：新任务照常入队，执行中任务自然跑完。 */
    public void pause(QuotaKey key) {
        State s = state(key);
        synchronized (s) {
            s.paused = true;
        }
    }

    /** 恢复某维度派发。 */
    public void resume(QuotaKey key) {
        State s = state(key);
        synchronized (s) {
            s.paused = false;
        }
    }

    /** 暂停某个调用方整体派发：其下所有组的维度都不再启动新任务。 */
    public void pauseCaller(String callerId) {
        pausedCallers.add(callerId);
    }

    public void resumeCaller(String callerId) {
        pausedCallers.remove(callerId);
    }

    public boolean isCallerPaused(String callerId) {
        return pausedCallers.contains(callerId);
    }

    /** 暂停某个任务组整体派发：其下所有 caller 的维度都不再启动新任务。 */
    public void pauseGroup(String group) {
        pausedGroups.add(group);
    }

    public void resumeGroup(String group) {
        pausedGroups.remove(group);
    }

    public boolean isGroupPaused(String group) {
        return pausedGroups.contains(group);
    }

    /**
     * 某运行维度当前是否暂停派发：精确维度、调用方整体、任务组整体
     * 任一作用域处于暂停即视为暂停（暂停是叠加语义，互不低消）。
     */
    public boolean isPaused(QuotaKey key) {
        if (pausedCallers.contains(key.callerId()) || pausedGroups.contains(key.group())) {
            return true;
        }
        State s = state(key);
        synchronized (s) {
            return s.paused;
        }
    }

    /** 调用方整体作用域当前生效的配额（无覆盖时回落默认值，用于运行态展示）。 */
    public QuotaLimits callerLimits(String callerId) {
        QuotaLimits override = callerOverrides.get(callerId);
        return override != null ? override : defaultLimits;
    }

    /** 任务组整体作用域当前生效的配额（无覆盖时回落默认值，用于运行态展示）。 */
    public QuotaLimits groupLimits(String group) {
        QuotaLimits override = groupOverrides.get(group);
        return override != null ? override : defaultLimits;
    }

    /** 当前已知（有计量状态或有配额覆盖）的全部维度。 */
    public java.util.Set<QuotaKey> knownKeys() {
        java.util.Set<QuotaKey> keys = new java.util.LinkedHashSet<>(states.keySet());
        keys.addAll(overrides.keySet());
        return keys;
    }

    /** 当前已知有治理状态的调用方整体作用域（配额覆盖或暂停）。 */
    public java.util.Set<String> knownCallerScopes() {
        java.util.Set<String> callers = new java.util.LinkedHashSet<>(callerOverrides.keySet());
        callers.addAll(pausedCallers);
        return callers;
    }

    /** 当前已知有治理状态的任务组整体作用域（配额覆盖或暂停）。 */
    public java.util.Set<String> knownGroupScopes() {
        java.util.Set<String> groups = new java.util.LinkedHashSet<>(groupOverrides.keySet());
        groups.addAll(pausedGroups);
        return groups;
    }
}
