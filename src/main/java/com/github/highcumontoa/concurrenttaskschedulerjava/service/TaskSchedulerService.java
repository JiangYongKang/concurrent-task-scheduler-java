package com.github.highcumontoa.concurrenttaskschedulerjava.service;

import com.github.highcumontoa.concurrenttaskschedulerjava.config.SchedulerProperties;
import com.github.highcumontoa.concurrenttaskschedulerjava.governance.GovernanceChangeResult;
import com.github.highcumontoa.concurrenttaskschedulerjava.governance.GovernanceEvent;
import com.github.highcumontoa.concurrenttaskschedulerjava.governance.GovernanceScope;
import com.github.highcumontoa.concurrenttaskschedulerjava.governance.GovernanceStatus;
import com.github.highcumontoa.concurrenttaskschedulerjava.governance.GovernanceStore;
import com.github.highcumontoa.concurrenttaskschedulerjava.handler.HandlerRegistry;
import com.github.highcumontoa.concurrenttaskschedulerjava.handler.TaskContext;
import com.github.highcumontoa.concurrenttaskschedulerjava.handler.TaskHandler;
import com.github.highcumontoa.concurrenttaskschedulerjava.model.RejectReason;
import com.github.highcumontoa.concurrenttaskschedulerjava.model.TaskRecord;
import com.github.highcumontoa.concurrenttaskschedulerjava.model.TaskStatus;
import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaKey;
import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaLimits;
import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaManager;
import com.github.highcumontoa.concurrenttaskschedulerjava.retry.ErrorClassification;
import com.github.highcumontoa.concurrenttaskschedulerjava.retry.ExponentialBackoffRetryPolicy;
import com.github.highcumontoa.concurrenttaskschedulerjava.retry.RetryPolicy;
import com.github.highcumontoa.concurrenttaskschedulerjava.store.TaskStore;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 配额感知的本地异步任务调度服务。
 *
 * <p>一致性要点：
 * <ul>
 *   <li>单一内部锁 {@code lock} 串行化所有状态转移与配额判定，杜绝 TOCTOU；</li>
 *   <li>状态先落 WAL 再对外可见；提交先持久化再占用排队配额，重复提交命中已有记录；</li>
 *   <li>并发槽位在“QUEUED/PENDING_RETRY -> RUNNING”时原子占用，终态或回到排队态时释放，
 *       取消/超时/失败路径都保证释放一次且仅一次，重启后按 WAL reinitialize；</li>
 *   <li>全局固定大小 worker 池 + SynchronousQueue handoff，构成总并发硬上限；</li>
 *   <li>取消/超时通过 Thread.interrupt 及时停止；任务在同一时刻最多只有一个执行实例。</li>
 * </ul>
 */
public class TaskSchedulerService {

    private static final Logger log = LoggerFactory.getLogger(TaskSchedulerService.class);

    private final TaskStore store;
    private final GovernanceStore governanceStore;
    private final QuotaManager quotaManager;
    private final HandlerRegistry handlerRegistry;
    private final RetryPolicy retryPolicy;
    private final long defaultTimeoutMillis;

    private final Object lock = new Object();
    private final Map<String, TaskRecord> tasks = new LinkedHashMap<>();
    /** 正在执行的运行句柄（taskId -> handle）。 */
    private final Map<String, RunningTask> running = new LinkedHashMap<>();

    private final ThreadPoolExecutor workers;
    private final ScheduledExecutorService timers;
    private final ScheduledExecutorService dispatcher;
    private volatile boolean started;
    private volatile boolean shutdown;
    /** 治理事件序号：启动回放后取已有最大值，之后在服务锁内单调递增。 */
    private long governanceSeq;

    private record RunningTask(Thread workerThread, Future<?> future) {
    }

    public TaskSchedulerService(TaskStore store, GovernanceStore governanceStore,
                                QuotaManager quotaManager,
                                HandlerRegistry handlerRegistry, SchedulerProperties props) {
        this.store = store;
        this.governanceStore = governanceStore;
        this.quotaManager = quotaManager;
        this.handlerRegistry = handlerRegistry;
        this.retryPolicy = new ExponentialBackoffRetryPolicy(
                props.getDefaultMaxRetries(), props.getBackoffBaseMillis(),
                props.getBackoffMultiplier(), props.getBackoffCapMillis(),
                props.getBackoffJitter());
        this.defaultTimeoutMillis = props.getDefaultTimeoutMillis();

        int poolSize = Math.max(1, props.getWorkerThreads());
        ThreadFactory workerFactory = numbered("task-worker-");
        // core == max 且 SynchronousQueue：没有空闲 worker 时直接 handoff 失败，
        // 全局并发被严格限制在 poolSize，任务不会在池内隐藏排队。
        this.workers = new ThreadPoolExecutor(poolSize, poolSize,
                0L, TimeUnit.MILLISECONDS,
                new java.util.concurrent.SynchronousQueue<>(), workerFactory,
                new ThreadPoolExecutor.AbortPolicy());
        this.timers = Executors.newSingleThreadScheduledExecutor(numbered("task-timer-"));
        this.dispatcher = Executors.newSingleThreadScheduledExecutor(numbered("task-dispatch-"));
    }

    private static ThreadFactory numbered(String prefix) {
        AtomicInteger seq = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    /** 从 WAL 恢复并启动派发循环（Bean 初始化后自动执行）。 */
    @PostConstruct
    public void start() {
        synchronized (lock) {
            if (started) {
                return;
            }
            started = true;
            Map<QuotaKey, int[]> counts = new LinkedHashMap<>();
            for (TaskRecord r : store.loadAll()) {
                tasks.put(r.getTaskId(), r);
                QuotaKey key = QuotaKey.of(r.getCallerId(), r.getGroup());
                int[] c = counts.computeIfAbsent(key, k -> new int[2]);
                switch (r.getStatus()) {
                    case RUNNING -> {
                        // 崩溃发生在执行中：任务已丢失执行体，保守地按可重试失败重新排队，
                        // 不置成功、不静默丢弃；active 计数不恢复（当前确无执行体）。
                        r.setStatus(TaskStatus.QUEUED);
                        r.setNextEligibleTime(0L);
                        r.setErrorReason("recovered after restart while RUNNING");
                        store.append(r);
                        c[0]++;
                        log.info("恢复 RUNNING 任务为 QUEUED: taskId={}", r.getTaskId());
                    }
                    case PENDING_RETRY, QUEUED -> {
                        if (r.getStatus() == TaskStatus.PENDING_RETRY) {
                            // 退避计时以绝对时间记录，重启后仍然有效；已到期则立即可派发。
                        }
                        c[0]++;
                    }
                    case SUCCEEDED, FAILED, CANCELLED -> {
                        // 终态：不占任何配额
                    }
                }
            }
            counts.forEach((key, c) -> quotaManager.reinitialize(key, c[0], c[1]));
            replayGovernanceEvents();
            dispatcher.scheduleWithFixedDelay(this::safeDispatch, 5, 10, TimeUnit.MILLISECONDS);
            log.info("调度器已启动: 恢复任务 {} 个, workerThreads={}", tasks.size(),
                    workers.getMaximumPoolSize());
        }
    }

    /**
     * 回放治理事件日志：按写入顺序应用，每个作用域以最后一条事件为准，
     * 使运行期配额调整与暂停状态在重启后保持，不会因重启自动恢复放量。
     */
    private void replayGovernanceEvents() {
        if (governanceStore == null) {
            return;
        }
        int applied = 0;
        for (GovernanceEvent e : governanceStore.loadAll()) {
            GovernanceScope.Kind kind;
            try {
                kind = GovernanceScope.Kind.valueOf(e.getKind());
            } catch (IllegalArgumentException bad) {
                log.warn("忽略无法识别作用域类型的治理事件: {}", e.getKind());
                continue;
            }
            GovernanceScope scope = new GovernanceScope(kind, e.getCallerId(), e.getGroup());
            if (e.getLimits() != null) {
                quotaManager.applyRuntimeLimits(scope, e.getLimits());
            }
            quotaManager.setPaused(scope, e.isPaused());
            quotaManager.recordLastChange(scope, e.getTimeMillis(),
                    describeChange(scope, e.isPaused(), e.getLimits(), e.getReason()));
            governanceSeq = Math.max(governanceSeq, e.getSeq());
            applied++;
        }
        if (applied > 0) {
            log.info("治理状态回放完成: {} 条事件, 最新序号={}", applied, governanceSeq);
        }
    }

    /** 构造“最近一次操作”的可读描述（落盘并在状态查询中展示）。 */
    private static String describeChange(GovernanceScope scope, boolean paused,
                                         com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaLimits limits,
                                         String reason) {
        StringBuilder sb = new StringBuilder();
        if (limits != null) {
            sb.append("adjust-limits maxConcurrency=").append(limits.maxConcurrency())
                    .append(" rateLimitPerSecond=").append(limits.rateLimitPerSecond())
                    .append(" maxQueued=").append(limits.maxQueued());
        }
        if (sb.length() > 0) {
            sb.append(' ');
        }
        sb.append("paused=").append(paused);
        if (reason != null && !reason.isBlank()) {
            sb.append(" reason=").append(reason);
        }
        sb.append(" scope=").append(scope.kind())
                .append('(').append(scope.callerId() == null ? "*" : scope.callerId())
                .append(',').append(scope.group() == null ? "*" : scope.group()).append(')');
        return sb.toString();
    }

    /** 优雅关闭（容器销毁时执行）。 */
    @PreDestroy
    public void shutdown() {
        shutdown = true;
        dispatcher.shutdownNow();
        timers.shutdownNow();
        workers.shutdownNow();
        try {
            workers.awaitTermination(1, java.util.concurrent.TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ---------------------------------------------------------------- 提交

    public SubmitResult submit(String taskId, String callerId, String group, String taskType,
                               String payload, Long timeoutMillis) {
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("taskId must not be blank");
        }
        if (callerId == null || callerId.isBlank()) {
            throw new IllegalArgumentException("callerId must not be blank");
        }
        if (taskType == null || taskType.isBlank()) {
            throw new IllegalArgumentException("taskType must not be blank");
        }
        if (group == null || group.isBlank()) {
            group = "default";
        }
        if (handlerRegistry.find(taskType).isEmpty()) {
            log.info("提交拒绝 taskId={} caller={} group={} reason=UNKNOWN_TASK_TYPE type={}",
                    taskId, callerId, group, taskType);
            return SubmitResult.rejected(RejectReason.UNKNOWN_TASK_TYPE,
                    "unknown task type: " + taskType);
        }
        QuotaKey key = QuotaKey.of(callerId, group);
        synchronized (lock) {
            TaskRecord existing = tasks.get(taskId);
            if (existing != null) {
                log.info("重复提交 taskId={} 当前状态={} -> 不重复执行/不重复占配额",
                        taskId, existing.getStatus());
                return SubmitResult.duplicate(taskId, existing.getStatus());
            }
            // 先判定队列配额，判定与占用在同一临界区内原子完成。
            if (!quotaManager.tryEnqueue(key)) {
                QuotaLimits l = quotaManager.limits(key);
                log.info("提交拒绝 taskId={} caller={} group={} reason=QUOTA_EXHAUSTED "
                                + "queued={} maxQueued={}", taskId, callerId, group,
                        quotaManager.queuedCount(key), l.maxQueued());
                return SubmitResult.rejected(RejectReason.QUOTA_EXHAUSTED,
                        "queue quota exhausted for " + key);
            }
            TaskRecord r = new TaskRecord();
            r.setTaskId(taskId);
            r.setCallerId(callerId);
            r.setGroup(group);
            r.setTaskType(taskType);
            r.setPayload(payload);
            r.setStatus(TaskStatus.QUEUED);
            r.setAttempts(0);
            r.setEnqueueTime(System.currentTimeMillis());
            r.setTimeoutMillis(timeoutMillis != null ? timeoutMillis : defaultTimeoutMillis);
            r.setVersion(1);
            // 先持久化再放入内存：保证“接受了就不会丢”。
            store.append(r);
            tasks.put(taskId, r);
            log.info("任务已入队 taskId={} caller={} group={} type={} 配额判定通过 "
                    + "queued={}", taskId, callerId, group, taskType,
                    quotaManager.queuedCount(key));
        }
        // 锁外唤醒，避免在锁内执行线程 handoff。
        safeDispatch();
        TaskRecord snapshot;
        synchronized (lock) {
            snapshot = tasks.get(taskId);
        }
        return SubmitResult.accepted(taskId, snapshot.getStatus());
    }

    // ---------------------------------------------------------------- 查询/取消

    public Optional<TaskRecord> get(String taskId) {
        synchronized (lock) {
            return Optional.ofNullable(tasks.get(taskId));
        }
    }

    public List<TaskRecord> list() {
        synchronized (lock) {
            return List.copyOf(tasks.values());
        }
    }

    public boolean cancel(String taskId, String reason) {
        RunningTask handle;
        synchronized (lock) {
            TaskRecord r = tasks.get(taskId);
            if (r == null) {
                return false;
            }
            if (isTerminal(r.getStatus())) {
                log.info("取消忽略 taskId={} 已处于终态 {}", taskId, r.getStatus());
                return false;
            }
            if (r.getStatus() == TaskStatus.RUNNING) {
                handle = running.get(taskId);
                r.setCancelRequested(true);
                r.setErrorReason("cancelled: " + safe(reason));
                // 状态由执行线程在感知中断后收敛为 CANCELLED；这里只发信号。
                if (handle != null) {
                    if (handle.workerThread() != null) {
                        handle.workerThread().interrupt();
                    }
                    if (handle.future() != null) {
                        handle.future().cancel(true);
                    }
                }
                log.info("取消执行中任务 taskId={} 已发中断", taskId);
                return true;
            }
            // QUEUED / PENDING_RETRY：在临界区内直接终结，释放排队配额。
            quotaManager.releaseQueued(QuotaKey.of(r.getCallerId(), r.getGroup()));
            r.setStatus(TaskStatus.CANCELLED);
            r.setEndTime(System.currentTimeMillis());
            r.setErrorReason("cancelled while queued: " + safe(reason));
            r.setVersion(r.getVersion() + 1);
            store.append(r);
            log.info("排队任务已取消 taskId={} 状态->CANCELLED", taskId);
        }
        safeDispatch();
        return true;
    }

    // ---------------------------------------------------------------- 派发

    private void safeDispatch() {
        if (shutdown) {
            return;
        }
        try {
            dispatch();
        } catch (Throwable t) {
            log.error("派发循环异常", t);
        }
    }

    /**
     * 在一次扫描中按入队顺序尝试启动任务。
     * 对每个 key 做队头阻塞：队头不满足（配额/退避）则跳过该 key 的后续任务，
     * 既保证同 key FIFO 公平，又不影响其他 key；没有空闲 worker 时结束本轮。
     */
    private void dispatch() {
        Map<QuotaKey, String> blocked = new LinkedHashMap<>();
        while (true) {
            TaskRecord picked;
            synchronized (lock) {
                picked = null;
                long now = System.currentTimeMillis();
                for (TaskRecord r : tasks.values()) {
                    if (r.getStatus() != TaskStatus.QUEUED && r.getStatus() != TaskStatus.PENDING_RETRY) {
                        continue;
                    }
                    QuotaKey key = QuotaKey.of(r.getCallerId(), r.getGroup());
                    String blocker = blocked.get(key);
                    if (blocker != null) {
                        continue;
                    }
                    // 运行期暂停：该维度只排队不派发，已在执行的任务自然跑完；
                    // 与配额不足一样按队头处理，不影响其它维度，也不改变任务状态。
                    if (quotaManager.isPaused(key)) {
                        blocked.put(key, r.getTaskId());
                        if (log.isDebugEnabled()) {
                            log.debug("维度暂停保持排队 taskId={} caller={} group={}",
                                    r.getTaskId(), r.getCallerId(), r.getGroup());
                        }
                        continue;
                    }
                    if (r.getStatus() == TaskStatus.PENDING_RETRY && r.getNextEligibleTime() > now) {
                        blocked.put(key, r.getTaskId());
                        continue;
                    }
                    picked = r;
                    break;
                }
                if (picked == null) {
                    return;
                }
                QuotaKey key = QuotaKey.of(picked.getCallerId(), picked.getGroup());
                if (!quotaManager.tryAcquireActive(key)) {
                    QuotaLimits l = quotaManager.limits(key);
                    log.info("配额不足保持排队 taskId={} caller={} group={} active={}/{} "
                                    + "windowStarts={}/s queued={}", picked.getTaskId(),
                            picked.getCallerId(), picked.getGroup(),
                            quotaManager.activeCount(key), l.maxConcurrency(),
                            l.rateLimitPerSecond(), quotaManager.queuedCount(key));
                    blocked.put(key, picked.getTaskId());
                    picked = null;
                } else {
                    // 配额与槽位均已拿到：离开排队态，释放排队长度计数。
                    quotaManager.releaseQueued(key);
                    picked.setStatus(TaskStatus.RUNNING);
                    picked.setAttempts(picked.getAttempts() + 1);
                    picked.setStartTime(System.currentTimeMillis());
                    picked.setVersion(picked.getVersion() + 1);
                    store.append(picked);
                }
            }
            if (picked == null) {
                continue;
            }
            TaskRecord toRun = picked;
            boolean acceptedByPool;
            Future<?> submitted;
            try {
                submitted = workers.submit(() -> runTask(toRun.getTaskId()));
                acceptedByPool = true;
            } catch (java.util.concurrent.RejectedExecutionException rej) {
                submitted = null;
                acceptedByPool = false;
            }
            if (!acceptedByPool) {
                // 无空闲全局 worker：回滚本次占用与状态，等下一轮（10ms）重试。
                // 回滚不补偿排队计数：该任务此前已 releaseQueued，回滚时重新计为排队。
                synchronized (lock) {
                    QuotaKey key = QuotaKey.of(toRun.getCallerId(), toRun.getGroup());
                    quotaManager.releaseActive(key);
                    quotaManager.tryEnqueue(key);
                    if (toRun.getStatus() == TaskStatus.RUNNING) {
                        toRun.setStatus(TaskStatus.QUEUED);
                        toRun.setAttempts(Math.max(0, toRun.getAttempts() - 1));
                        toRun.setVersion(toRun.getVersion() + 1);
                        store.append(toRun);
                    }
                    log.info("全局 worker 已满 taskId={} 回滚为 QUEUED，等待下轮派发",
                            toRun.getTaskId());
                }
                return;
            }
            synchronized (lock) {
                // 占位句柄：执行线程在 runTask 开头替换为真实线程。
                running.put(toRun.getTaskId(), new RunningTask(null, submitted));
            }
        }
    }

    // ---------------------------------------------------------------- 执行

    private void runTask(String taskId) {
        CURRENT_TASK.set(taskId);
        Thread executionThread = Thread.currentThread();
        TaskRecord r;
        TaskHandler handler;
        int attempt;
        long timeoutMillis;
        synchronized (lock) {
            r = tasks.get(taskId);
            if (r == null) {
                return;
            }
            // 用真正的执行线程替换派发时登记的占位句柄。
            RunningTask placeholder = running.get(taskId);
            running.put(taskId, new RunningTask(executionThread,
                    placeholder == null ? null : placeholder.future()));
            handler = handlerRegistry.find(r.getTaskType()).orElse(null);
            attempt = r.getAttempts();
            timeoutMillis = r.getTimeoutMillis();
            r.setErrorReason(null); // 每次执行清空上一轮原因，由本次结果重新收敛
        }
        MDC.put("taskId", taskId);
        log.info("开始执行 taskId={} attempt={} caller={} group={} type={}", taskId, attempt,
                r.getCallerId(), r.getGroup(), r.getTaskType());

        // 超时定时器：到点中断执行线程，TIMEOUT 与业务异常/取消可区分。
        ScheduledTimeout timeout = null;
        if (timeoutMillis > 0) {
            timeout = new ScheduledTimeout();
            final ScheduledTimeout scheduled = timeout;
            scheduled.token = timers.schedule(() -> {
                scheduled.fired = true;
                executionThread.interrupt();
            }, timeoutMillis, TimeUnit.MILLISECONDS);
        }

        Outcome outcome;
        try {
            if (handler == null) {
                throw new IllegalStateException("handler missing: " + r.getTaskType());
            }
            TaskContext ctx = new ContextImpl(taskId, r, attempt);
            String result = handler.execute(r.getPayload(), ctx);
            if (timeout != null && timeout.fired) {
                outcome = new Outcome(ErrorClassification.TIMEOUT, result,
                        new java.util.concurrent.TimeoutException(
                                "execution exceeded " + timeoutMillis + "ms"));
            } else {
                outcome = Outcome.success(result);
            }
        } catch (Throwable t) {
            outcome = classifyFailure(t, timeout);
        } finally {
            if (timeout != null && timeout.token != null) {
                timeout.token.cancel(false);
            }
            // 清除可能由超时/取消设置的中断状态，避免污染 worker 线程的下次复用。
            Thread.interrupted();
        }
        finish(taskId, outcome);
        MDC.remove("taskId");
        // 执行结束可能释放配额，立即触发一轮派发。
        safeDispatch();
    }

    private static final class ScheduledTimeout {
        volatile java.util.concurrent.ScheduledFuture<?> token;
        volatile boolean fired;
    }

    private record Outcome(ErrorClassification classification, String result,
                           Throwable error) {
        static Outcome success(String result) {
            return new Outcome(null, result, null);
        }
    }

    private Outcome classifyFailure(Throwable t, ScheduledTimeout timeout) {
        if (timeout != null && timeout.fired) {
            return new Outcome(ErrorClassification.TIMEOUT, null, t);
        }
        boolean cancelRequested;
        synchronized (lock) {
            TaskRecord cur = tasks.get(CURRENT_TASK.get());
            cancelRequested = cur != null && cur.isCancelRequested();
        }
        if (t instanceof InterruptedException
                || (t instanceof java.util.concurrent.CancellationException)
                || cancelRequested) {
            return new Outcome(ErrorClassification.CANCELLED, null, t);
        }
        return new Outcome(retryPolicy.classify(t), null, t);
    }

    private static final ThreadLocal<String> CURRENT_TASK = new ThreadLocal<>();

    /** 收敛一次执行后的状态：成功/失败/重试/取消，全部在锁内完成并释放一次配额。 */
    private void finish(String taskId, Outcome outcome) {
        synchronized (lock) {
            TaskRecord r = tasks.get(taskId);
            if (r == null) {
                return;
            }
            running.remove(taskId);
            QuotaKey key = QuotaKey.of(r.getCallerId(), r.getGroup());
            long now = System.currentTimeMillis();

            // 取消优先：即使业务同时抛错，只要已请求取消且任务尚未终结，即为 CANCELLED。
            boolean cancelRequested = r.isCancelRequested();

            if (outcome.classification() == null) {
                quotaManager.releaseActive(key);
                r.setStatus(TaskStatus.SUCCEEDED);
                r.setResult(outcome.result());
                r.setErrorReason(null);
                r.setErrorClass(null);
                r.setEndTime(now);
                r.setVersion(r.getVersion() + 1);
                store.append(r);
                log.info("执行成功 taskId={} attempt={} 状态->SUCCEEDED 释放并发槽位 active={}",
                        taskId, r.getAttempts(), quotaManager.activeCount(key));
                return;
            }

            ErrorClassification classification = outcome.classification();
            if (cancelRequested && classification != ErrorClassification.TIMEOUT) {
                classification = ErrorClassification.CANCELLED;
            }
            Throwable err = outcome.error();
            String reason = explain(classification, err);
            String errClass = err == null ? classification.name() : err.getClass().getName();

            if (classification == ErrorClassification.CANCELLED) {
                quotaManager.releaseActive(key);
                r.setStatus(TaskStatus.CANCELLED);
                String cancelReason = r.getErrorReason() != null && r.getErrorReason().startsWith("cancelled")
                        ? r.getErrorReason()
                        : "cancelled: interrupted";
                r.setErrorReason(cancelReason);
                r.setCancelRequested(false);
                r.setErrorClass(errClass);
                r.setEndTime(now);
                r.setVersion(r.getVersion() + 1);
                store.append(r);
                log.info("任务取消 taskId={} attempt={} 状态->CANCELLED active={}", taskId,
                        r.getAttempts(), quotaManager.activeCount(key));
                return;
            }

            if (classification == ErrorClassification.TIMEOUT) {
                quotaManager.releaseActive(key);
                r.setStatus(TaskStatus.FAILED);
                r.setErrorReason(reason);
                r.setErrorClass(errClass);
                r.setEndTime(now);
                r.setVersion(r.getVersion() + 1);
                store.append(r);
                log.warn("任务超时失败 taskId={} attempt={} timeoutMs={} 状态->FAILED "
                        + "(不重试) active={}", taskId, r.getAttempts(), r.getTimeoutMillis(),
                        quotaManager.activeCount(key));
                return;
            }

            if (classification == ErrorClassification.NON_RETRYABLE
                    || r.getAttempts() > retryPolicy.maxRetries()) {
                quotaManager.releaseActive(key);
                r.setStatus(TaskStatus.FAILED);
                r.setErrorReason(reason);
                r.setErrorClass(errClass);
                r.setEndTime(now);
                r.setVersion(r.getVersion() + 1);
                store.append(r);
                log.warn("任务最终失败 taskId={} attempts={} 分类={} 原因={} 状态->FAILED "
                        + "active={}", taskId, r.getAttempts(), classification, reason,
                        quotaManager.activeCount(key));
                return;
            }

            // RETRYABLE 且仍有重试额度：释放并发槽位，进入 PENDING_RETRY 退避，
            // 重新占用一个排队名额，退避到期后由派发器重新启动。
            quotaManager.releaseActive(key);
            quotaManager.tryEnqueue(key);
            long backoff = retryPolicy.backoffMillis(r.getAttempts());
            r.setStatus(TaskStatus.PENDING_RETRY);
            r.setNextEligibleTime(now + backoff);
            r.setErrorReason(reason);
            r.setErrorClass(errClass);
            r.setVersion(r.getVersion() + 1);
            store.append(r);
            log.info("任务可重试失败 taskId={} attempt={}/{} 退避={}ms 下次可执行={} "
                    + "状态->PENDING_RETRY active={}", taskId, r.getAttempts(),
                    retryPolicy.maxRetries() + 1, backoff, r.getNextEligibleTime(),
                    quotaManager.activeCount(key));
            // 安排退避到期唤醒。
            timers.schedule(this::safeDispatch, Math.max(1, backoff + 2), TimeUnit.MILLISECONDS);
        }
    }

    private String explain(ErrorClassification classification, Throwable err) {
        String base = err == null || err.getMessage() == null
                ? classification.name() : err.getMessage();
        return switch (classification) {
            case TIMEOUT -> "execution timeout: " + base;
            case NON_RETRYABLE -> "non-retryable error: " + base;
            case CANCELLED -> "cancelled: " + base;
            case RETRYABLE -> "retryable error: " + base;
        };
    }

    private static boolean isTerminal(TaskStatus s) {
        return s == TaskStatus.SUCCEEDED || s == TaskStatus.FAILED || s == TaskStatus.CANCELLED;
    }

    // ---------------------------------------------------------------- 运行期治理

    /**
     * 运行期调整配额（PATCH：null 项保持不变）。非法值（负数）抛
     * {@link IllegalArgumentException}，原有效值保持不变。
     *
     * <p>调整立即参与后续调度判定：调小不中断在执行任务、不回退状态；
     * 调大后由 {@link #safeDispatch()} 按原 FIFO 尽快放行积压任务。
     */
    public GovernanceChangeResult adjustQuota(String callerId, String group,
                                              Integer maxConcurrency,
                                              Integer rateLimitPerSecond, Integer maxQueued,
                                              String reason) {
        if (maxConcurrency == null && rateLimitPerSecond == null && maxQueued == null) {
            throw new IllegalArgumentException(
                    "at least one of maxConcurrency/rateLimitPerSecond/maxQueued is required");
        }
        if (maxConcurrency != null && maxConcurrency < 0) {
            throw new IllegalArgumentException("maxConcurrency must be >= 0 (0 = unlimited)");
        }
        if (rateLimitPerSecond != null && rateLimitPerSecond < 0) {
            throw new IllegalArgumentException("rateLimitPerSecond must be >= 0 (0 = unlimited)");
        }
        if (maxQueued != null && maxQueued < 0) {
            throw new IllegalArgumentException("maxQueued must be >= 0 (0 = unlimited)");
        }
        GovernanceScope scope = resolveScope(callerId, group);
        GovernanceChangeResult result;
        synchronized (lock) {
            // PATCH 基准为该作用域自身的当前运行期值（首次调整取默认配置），
            // 宽作用域不能借用某个精确维度的生效值，避免串值。
            QuotaLimits current = quotaManager.scopeBaselineLimits(scope);
            QuotaLimits next = new QuotaLimits(
                    maxConcurrency != null ? maxConcurrency : current.maxConcurrency(),
                    rateLimitPerSecond != null ? rateLimitPerSecond
                            : current.rateLimitPerSecond(),
                    maxQueued != null ? maxQueued : current.maxQueued());
            // 先持久化再改内存状态，保证“生效即可重启恢复”。
            long now = System.currentTimeMillis();
            GovernanceEvent event = persistGovernanceEvent(scope, next,
                    quotaManager.scopePaused(scope), reason, now);
            quotaManager.applyRuntimeLimits(scope, next);
            quotaManager.recordLastChange(scope, event.getTimeMillis(),
                    describeChange(scope, event.isPaused(), next, reason));
            result = new GovernanceChangeResult(scope, event.isPaused(), next,
                    event.getTimeMillis(), reason);
            log.info("运行期配额调整生效 scope={} caller={} group={} -> maxConcurrency={} "
                            + "rateLimitPerSecond={} maxQueued={} seq={}", scope.kind(),
                    scope.callerId(), scope.group(), next.maxConcurrency(),
                    next.rateLimitPerSecond(), next.maxQueued(), event.getSeq());
        }
        // 调大可能释放积压：锁外唤醒。
        safeDispatch();
        return result;
    }

    /** 暂停某维度派发：新任务照常受理排队，已在执行的自然跑完；状态跨重启保持。 */
    public GovernanceChangeResult pauseDispatch(String callerId, String group, String reason) {
        return setDispatchPaused(callerId, group, true, reason);
    }

    /** 恢复某维度派发：排队任务按原公平顺序继续。 */
    public GovernanceChangeResult resumeDispatch(String callerId, String group, String reason) {
        return setDispatchPaused(callerId, group, false, reason);
    }

    private GovernanceChangeResult setDispatchPaused(String callerId, String group,
                                                     boolean paused, String reason) {
        GovernanceScope scope = resolveScope(callerId, group);
        GovernanceChangeResult result;
        synchronized (lock) {
            long now = System.currentTimeMillis();
            QuotaLimits limits = quotaManager.scopeBaselineLimits(scope);
            GovernanceEvent event = persistGovernanceEvent(scope, null, paused, reason, now);
            quotaManager.setPaused(scope, paused);
            quotaManager.recordLastChange(scope, event.getTimeMillis(),
                    describeChange(scope, paused, null, reason));
            result = new GovernanceChangeResult(scope, paused, limits, event.getTimeMillis(),
                    reason);
            log.info("运行期派发{} scope={} caller={} group={} seq={} reason={}",
                    paused ? "暂停" : "恢复", scope.kind(), scope.callerId(), scope.group(),
                    event.getSeq(), reason);
        }
        if (!paused) {
            // 恢复后立即尝试按 FIFO 放行。
            safeDispatch();
        }
        return result;
    }

    /** 查询某精确维度（caller + group）当前运行态；数字在锁内一次性取齐。 */
    public GovernanceStatus governanceStatus(String callerId, String group) {
        if (callerId == null || callerId.isBlank()) {
            throw new IllegalArgumentException("callerId must not be blank");
        }
        if (group == null || group.isBlank()) {
            group = "default";
        }
        QuotaKey key = QuotaKey.of(callerId, group);
        synchronized (lock) {
            return new GovernanceStatus(callerId, group,
                    quotaManager.activeCount(key), quotaManager.queuedCount(key),
                    quotaManager.startedInCurrentWindow(key), quotaManager.limits(key),
                    quotaManager.isPaused(key),
                    quotaManager.lastChangeTimeMillis(callerId, group),
                    quotaManager.lastChangeDescription(callerId, group));
        }
    }

    /**
     * 解析治理作用域：两者都给=精确；只给 caller=调用方；只给 group=任务组；
     * 都不给非法。group 缺省 "default"。
     */
    private static GovernanceScope resolveScope(String callerId, String group) {
        boolean hasCaller = callerId != null && !callerId.isBlank();
        boolean hasGroup = group != null && !group.isBlank();
        if (!hasCaller && !hasGroup) {
            throw new IllegalArgumentException(
                    "callerId and group must not both be blank; specify at least one dimension");
        }
        if (hasCaller && hasGroup) {
            return GovernanceScope.exact(callerId.trim(), group.trim());
        }
        if (hasCaller) {
            return GovernanceScope.caller(callerId.trim());
        }
        return GovernanceScope.group(group.trim());
    }

    /** 分配序号、写治理事件日志；调用方必须持有服务锁。 */
    private GovernanceEvent persistGovernanceEvent(GovernanceScope scope,
                                                   QuotaLimits limits, boolean paused,
                                                   String reason, long now) {
        governanceSeq++;
        GovernanceEvent event = new GovernanceEvent();
        event.setSeq(governanceSeq);
        event.setTimeMillis(now);
        event.setKind(scope.kind().name());
        event.setCallerId(scope.callerId());
        event.setGroup(scope.group());
        event.setPaused(paused);
        event.setLimits(limits);
        event.setReason(reason);
        if (governanceStore != null) {
            governanceStore.append(event);
        }
        return event;
    }

    private static String safe(String s) {
        return s == null ? "" : s;
    }

    /** 处理器可见的执行上下文；cancelled 同时响应显式取消与执行线程中断。 */
    private final class ContextImpl implements TaskContext {
        private final String taskId;
        private final TaskRecord record;
        private final int attempt;

        private ContextImpl(String taskId, TaskRecord record, int attempt) {
            this.taskId = taskId;
            this.record = record;
            this.attempt = attempt;
        }

        @Override public String taskId() { return taskId; }
        @Override public String callerId() { return record.getCallerId(); }
        @Override public String group() { return record.getGroup(); }
        @Override public int attempt() { return attempt; }

        @Override
        public boolean cancelled() {
            synchronized (lock) {
                return record.getStatus() != TaskStatus.RUNNING || record.isCancelRequested()
                        || Thread.currentThread().isInterrupted();
            }
        }
    }
}
