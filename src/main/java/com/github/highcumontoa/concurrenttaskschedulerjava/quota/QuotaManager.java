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
}
