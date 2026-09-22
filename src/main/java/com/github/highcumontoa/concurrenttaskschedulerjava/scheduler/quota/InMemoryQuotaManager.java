package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.quota;

import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.config.SchedulerProperties;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.QuotaSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 内存配额管理器。
 *
 * <p>每个作用域（全局 + 每个调用方）维护三类资源：</p>
 * <ul>
 *   <li><b>并发槽位</b> running：当前执行中的任务数，上限 maxConcurrency；</li>
 *   <li><b>队列槽位</b> queued：当前排队中的任务数，上限 maxQueued；</li>
 *   <li><b>速率令牌</b> token bucket：按 rateLimitPerSecond 持续补充，
 *       任务<b>启动执行</b>时消耗一个令牌，排队不消耗。</li>
 * </ul>
 *
 * <p>所有「判定 + 记账」在同一把锁内原子完成，保证绝不越限；
 * 令牌桶的补充基于惰性计算（读取时按经过时间补发），无需后台线程。</p>
 */
public class InMemoryQuotaManager implements QuotaManager {

    private static final Logger log = LoggerFactory.getLogger(InMemoryQuotaManager.class);

    private static final class Bucket {
        int running;
        int queued;
        double tokens;
        long lastRefillNanos;

        Bucket(double initialTokens) {
            this.tokens = initialTokens;
            this.lastRefillNanos = System.nanoTime();
        }
    }

    private final SchedulerProperties props;
    private final Object lock = new Object();
    private final Bucket global;
    private final Map<String, Bucket> callers = new HashMap<>();

    public InMemoryQuotaManager(SchedulerProperties props) {
        this.props = props;
        this.global = new Bucket(props.getGlobalRateLimitPerSecond());
    }

    // ---------- 配额解析 ----------

    private int callerMaxConcurrency(String caller) {
        SchedulerProperties.CallerQuota c = props.getCallers().get(caller);
        if (c != null && c.getMaxConcurrency() != null) {
            return c.getMaxConcurrency();
        }
        return props.getDefaultMaxConcurrency();
    }

    private int callerMaxQueued(String caller) {
        SchedulerProperties.CallerQuota c = props.getCallers().get(caller);
        if (c != null && c.getMaxQueued() != null) {
            return c.getMaxQueued();
        }
        return props.getDefaultMaxQueued();
    }

    private int callerRate(String caller) {
        SchedulerProperties.CallerQuota c = props.getCallers().get(caller);
        if (c != null && c.getRateLimitPerSecond() != null) {
            return c.getRateLimitPerSecond();
        }
        return props.getDefaultRateLimitPerSecond();
    }

    private Bucket caller(String caller) {
        return callers.computeIfAbsent(caller,
                k -> new Bucket(callerRate(k)));
    }

    private void refill(Bucket b, int ratePerSecond) {
        long now = System.nanoTime();
        if (ratePerSecond <= 0) {
            b.tokens = 0;
            b.lastRefillNanos = now;
            return;
        }
        double elapsedSeconds = (now - b.lastRefillNanos) / 1_000_000_000.0;
        if (elapsedSeconds > 0) {
            b.tokens = Math.min(ratePerSecond, b.tokens + elapsedSeconds * ratePerSecond);
            b.lastRefillNanos = now;
        }
    }

    // ---------- 提交路径 ----------

    @Override
    public boolean tryAcquireRunning(String caller) {
        synchronized (lock) {
            Bucket cb = caller(caller);
            refill(global, props.getGlobalRateLimitPerSecond());
            refill(cb, callerRate(caller));

            if (cb.running >= callerMaxConcurrency(caller)) {
                return false;
            }
            if (global.running >= props.getGlobalMaxConcurrency()) {
                return false;
            }
            if (cb.tokens < 1.0 || global.tokens < 1.0) {
                return false;
            }
            cb.running++;
            global.running++;
            cb.tokens -= 1.0;
            global.tokens -= 1.0;
            log.debug("配额判定 START caller={} callerRunning={}/{} globalRunning={}/{} callerTokens~{}/gTokens~{}",
                    caller, cb.running, callerMaxConcurrency(caller), global.running,
                    props.getGlobalMaxConcurrency(),
                    String.format("%.2f", cb.tokens), String.format("%.2f", global.tokens));
            return true;
        }
    }

    @Override
    public boolean tryAcquireQueued(String caller) {
        synchronized (lock) {
            Bucket cb = caller(caller);
            if (cb.queued >= callerMaxQueued(caller)) {
                return false;
            }
            if (global.queued >= props.getGlobalMaxQueued()) {
                return false;
            }
            cb.queued++;
            global.queued++;
            log.debug("配额判定 ENQUEUE caller={} callerQueued={}/{} globalQueued={}/{}",
                    caller, cb.queued, callerMaxQueued(caller), global.queued,
                    props.getGlobalMaxQueued());
            return true;
        }
    }

    // ---------- 派发路径 ----------

    @Override
    public boolean tryPromoteQueuedToRunning(String caller) {
        synchronized (lock) {
            Bucket cb = caller(caller);
            refill(global, props.getGlobalRateLimitPerSecond());
            refill(cb, callerRate(caller));

            if (cb.running >= callerMaxConcurrency(caller)) {
                return false;
            }
            if (global.running >= props.getGlobalMaxConcurrency()) {
                return false;
            }
            if (cb.tokens < 1.0 || global.tokens < 1.0) {
                return false;
            }
            // 先占运行名额与令牌
            cb.running++;
            global.running++;
            cb.tokens -= 1.0;
            global.tokens -= 1.0;
            // 再释放该任务此前占用的队列名额（若有）
            if (cb.queued > 0) {
                cb.queued--;
            }
            if (global.queued > 0) {
                global.queued--;
            }
            log.debug("配额判定 PROMOTE caller={} callerRunning={}/{} queued={} globalRunning={}/{} globalQueued={}",
                    caller, cb.running, callerMaxConcurrency(caller), cb.queued,
                    global.running, props.getGlobalMaxConcurrency(), global.queued);
            return true;
        }
    }

    // ---------- 释放路径 ----------

    @Override
    public void releaseRunning(String caller) {
        synchronized (lock) {
            Bucket cb = caller(caller);
            if (cb.running > 0) {
                cb.running--;
            }
            if (global.running > 0) {
                global.running--;
            }
            log.debug("配额释放 RUNNING caller={} callerRunning={} globalRunning={}",
                    caller, cb.running, global.running);
        }
    }

    @Override
    public void releaseQueued(String caller) {
        synchronized (lock) {
            Bucket cb = caller(caller);
            if (cb.queued > 0) {
                cb.queued--;
            }
            if (global.queued > 0) {
                global.queued--;
            }
            log.debug("配额释放 QUEUED caller={} callerQueued={} globalQueued={}",
                    caller, cb.queued, global.queued);
        }
    }

    // ---------- 重启恢复 ----------

    @Override
    public void recover(int running, Map<String, Integer> runningByCaller,
                        int queued, Map<String, Integer> queuedByCaller) {
        synchronized (lock) {
            global.running = running;
            global.queued = queued;
            callers.clear();
            runningByCaller.forEach((c, n) -> caller(c).running = n);
            queuedByCaller.forEach((c, n) -> caller(c).queued = n);
            log.info("配额状态恢复: globalRunning={} globalQueued={} callers={}",
                    running, queued, callers.size());
        }
    }

    // ---------- 观测 ----------

    @Override
    public QuotaSnapshot globalSnapshot() {
        synchronized (lock) {
            refill(global, props.getGlobalRateLimitPerSecond());
            return new QuotaSnapshot(props.getGlobalMaxConcurrency(), global.running,
                    props.getGlobalMaxQueued(), global.queued,
                    props.getGlobalRateLimitPerSecond(), global.tokens);
        }
    }

    @Override
    public QuotaSnapshot callerSnapshot(String caller) {
        synchronized (lock) {
            Bucket cb = caller(caller);
            int rate = callerRate(caller);
            refill(cb, rate);
            return new QuotaSnapshot(callerMaxConcurrency(caller), cb.running,
                    callerMaxQueued(caller), cb.queued, rate, cb.tokens);
        }
    }
}
