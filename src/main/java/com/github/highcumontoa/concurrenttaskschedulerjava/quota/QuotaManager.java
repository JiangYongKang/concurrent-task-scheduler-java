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

    public QuotaLimits limits(QuotaKey key) {
        QuotaLimits override = overrides.get(key);
        return override != null ? override : defaultLimits;
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
        validate(key, limits);
        overrides.put(key, limits);
    }

    private static void validate(QuotaKey key, QuotaLimits limits) {
        if (key == null || limits == null) {
            throw new IllegalArgumentException("key and limits must not be null");
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

    public boolean isPaused(QuotaKey key) {
        State s = state(key);
        synchronized (s) {
            return s.paused;
        }
    }

    /** 当前已知（有计量状态或有配额覆盖）的全部维度。 */
    public java.util.Set<QuotaKey> knownKeys() {
        java.util.Set<QuotaKey> keys = new java.util.LinkedHashSet<>(states.keySet());
        keys.addAll(overrides.keySet());
        return keys;
    }
}
