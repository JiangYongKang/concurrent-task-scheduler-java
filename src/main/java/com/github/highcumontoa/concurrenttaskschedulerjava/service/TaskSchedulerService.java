package com.github.highcumontoa.concurrenttaskschedulerjava.service;

import com.github.highcumontoa.concurrenttaskschedulerjava.config.SchedulerProperties;
import com.github.highcumontoa.concurrenttaskschedulerjava.governance.GovernanceRecord;
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
import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaStatus;
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
    private final QuotaManager quotaManager;
    private final HandlerRegistry handlerRegistry;
    private final RetryPolicy retryPolicy;
    private final long defaultTimeoutMillis;
    private final GovernanceStore governanceStore;

    private final Object lock = new Object();
    private final Map<String, TaskRecord> tasks = new LinkedHashMap<>();
    /** 正在执行的运行句柄（taskId -> handle）。 */
    private final Map<String, RunningTask> running = new LinkedHashMap<>();
    /** 各维度最近一次治理操作（内存镜像，与治理 WAL 一致）。 */
    private final Map<QuotaKey, GovernanceRecord> governance = new LinkedHashMap<>();

    private final ThreadPoolExecutor workers;
    private final ScheduledExecutorService timers;
    private final ScheduledExecutorService dispatcher;
    private volatile boolean started;
    private volatile boolean shutdown;

    private record RunningTask(Thread workerThread, Future<?> future) {
    }

    public TaskSchedulerService(TaskStore store, QuotaManager quotaManager,
                                HandlerRegistry handlerRegistry, SchedulerProperties props,
                                GovernanceStore governanceStore) {
        this.store = store;
        this.quotaManager = quotaManager;
        this.handlerRegistry = handlerRegistry;
        this.governanceStore = governanceStore;
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
            // 回放治理 WAL：运行期配额覆盖与暂停状态跨重启保留，
            // 不会因为重启而自动恢复放量或丢失调整。
            for (GovernanceRecord g : governanceStore.loadAll()) {
                QuotaKey key = QuotaKey.of(g.getCallerId(), g.getGroup());
                try {
                    if (g.getLimits() != null) {
                        quotaManager.adjustLimits(key, g.getLimits());
                    }
                } catch (IllegalArgumentException invalid) {
                    log.warn("治理 WAL 中 {} 的配额覆盖非法，忽略: {}", key, invalid.getMessage());
                }
                if (g.isPaused()) {
                    quotaManager.pause(key);
                }
                governance.put(key, g);
                log.info("[治理] 恢复维度状态 caller={} group={} paused={} limits={} 最近操作={}@{}",
                        key.callerId(), key.group(), g.isPaused(), g.getLimits(),
                        g.getLastOperation(), g.getUpdatedAtEpochMillis());
            }
            dispatcher.scheduleWithFixedDelay(this::safeDispatch, 5, 10, TimeUnit.MILLISECONDS);
            log.info("调度器已启动: 恢复任务 {} 个, workerThreads={}", tasks.size(),
                    workers.getMaximumPoolSize());
        }
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

    // ---------------------------------------------------------------- 运行期治理

    /**
     * 运行期调整某维度三项配额，立即参与后续调度判定并持久化（重启后仍生效）。
     * 非法值抛出 {@link IllegalArgumentException}，原有生效值不变。
     * 调小并发上限不打断执行中任务；调大后立即触发派发放出积压。
     */
    public QuotaStatus adjustQuota(String callerId, String group, Integer maxConcurrency,
                                   Integer rateLimitPerSecond, Integer maxQueued) {
        QuotaKey key = governanceKey(callerId, group);
        if (maxConcurrency == null || rateLimitPerSecond == null || maxQueued == null) {
            throw new IllegalArgumentException(
                    "maxConcurrency/rateLimitPerSecond/maxQueued must all be provided");
        }
        QuotaLimits limits = new QuotaLimits(maxConcurrency, rateLimitPerSecond, maxQueued);
        synchronized (lock) {
            // 先校验再落盘再生效：非法值在这里抛出，原有生效值与持久化状态都不变。
            quotaManager.adjustLimits(key, limits);
            GovernanceRecord rec = mergedRecord(key);
            rec.setLimits(limits);
            rec.setLastOperation("ADJUST_QUOTA");
            rec.setLastOperationDetail("maxConcurrency=" + limits.maxConcurrency()
                    + ", rateLimitPerSecond=" + limits.rateLimitPerSecond()
                    + ", maxQueued=" + limits.maxQueued());
            rec.setUpdatedAtEpochMillis(System.currentTimeMillis());
            governanceStore.append(rec);
            governance.put(key, rec);
            log.info("[治理] 配额调整 caller={} group={} -> {} 当前 active={} queued={} paused={}",
                    key.callerId(), key.group(), rec.getLastOperationDetail(),
                    quotaManager.activeCount(key), quotaManager.queuedCount(key),
                    quotaManager.isPaused(key));
        }
        // 锁外触发派发：调大上限后积压任务按原顺序尽快放出。
        safeDispatch();
        return governanceStatus(callerId, group);
    }

    /** 暂停某维度派发：新任务照常受理排队，执行中任务自然跑完；状态持久化。 */
    public QuotaStatus pause(String callerId, String group) {
        QuotaKey key = governanceKey(callerId, group);
        synchronized (lock) {
            quotaManager.pause(key);
            GovernanceRecord rec = mergedRecord(key);
            rec.setPaused(true);
            rec.setLastOperation("PAUSE");
            rec.setLastOperationDetail("paused=true");
            rec.setUpdatedAtEpochMillis(System.currentTimeMillis());
            governanceStore.append(rec);
            governance.put(key, rec);
            log.info("[治理] 暂停派发 caller={} group={} 已入队任务继续排队 queued={} "
                    + "执行中任务自然跑完 active={}", key.callerId(), key.group(),
                    quotaManager.queuedCount(key), quotaManager.activeCount(key));
        }
        return governanceStatus(callerId, group);
    }

    /** 恢复某维度派发：排队任务按原公平顺序继续执行；状态持久化。 */
    public QuotaStatus resume(String callerId, String group) {
        QuotaKey key = governanceKey(callerId, group);
        synchronized (lock) {
            quotaManager.resume(key);
            GovernanceRecord rec = mergedRecord(key);
            rec.setPaused(false);
            rec.setLastOperation("RESUME");
            rec.setLastOperationDetail("paused=false");
            rec.setUpdatedAtEpochMillis(System.currentTimeMillis());
            governanceStore.append(rec);
            governance.put(key, rec);
            log.info("[治理] 恢复派发 caller={} group={} 积压 queued={} 将按原顺序放出",
                    key.callerId(), key.group(), quotaManager.queuedCount(key));
        }
        safeDispatch();
        return governanceStatus(callerId, group);
    }

    /** 查询某维度当前运行态（计数、生效配额、暂停状态、最近治理操作）。 */
    public QuotaStatus governanceStatus(String callerId, String group) {
        QuotaKey key = governanceKey(callerId, group);
        synchronized (lock) {
            return snapshotLocked(key);
        }
    }

    /** 列出所有已知维度的运行态。 */
    public List<QuotaStatus> listGovernanceStatus() {
        synchronized (lock) {
            java.util.Set<QuotaKey> keys = new java.util.TreeSet<>(
                    java.util.Comparator.comparing(QuotaKey::callerId)
                            .thenComparing(QuotaKey::group));
            keys.addAll(quotaManager.knownKeys());
            keys.addAll(governance.keySet());
            List<QuotaStatus> result = new java.util.ArrayList<>();
            for (QuotaKey key : keys) {
                result.add(snapshotLocked(key));
            }
            return result;
        }
    }

    /** 在锁内读取某维度的一致快照：计数与调度判定使用同一临界区。 */
    private QuotaStatus snapshotLocked(QuotaKey key) {
        QuotaLimits l = quotaManager.limits(key);
        GovernanceRecord g = governance.get(key);
        return new QuotaStatus(key.callerId(), key.group(),
                l.maxConcurrency(), l.rateLimitPerSecond(), l.maxQueued(),
                quotaManager.activeCount(key), quotaManager.queuedCount(key),
                quotaManager.startedInCurrentWindow(key),
                quotaManager.isPaused(key),
                g == null ? null : g.getLastOperation(),
                g == null ? 0L : g.getUpdatedAtEpochMillis(),
                g == null ? null : g.getLastOperationDetail());
    }

    /** 基于已有记录合并出完整治理状态（保留未涉及的字段，如暂停时保留配额覆盖）。 */
    private GovernanceRecord mergedRecord(QuotaKey key) {
        GovernanceRecord existing = governance.get(key);
        GovernanceRecord rec = new GovernanceRecord();
        rec.setCallerId(key.callerId());
        rec.setGroup(key.group());
        if (existing != null) {
            rec.setPaused(existing.isPaused());
            rec.setLimits(existing.getLimits());
        }
        return rec;
    }

    private static QuotaKey governanceKey(String callerId, String group) {
        if (callerId == null || callerId.isBlank()) {
            throw new IllegalArgumentException("callerId must not be blank");
        }
        if (group == null || group.isBlank()) {
            group = "default";
        }
        return QuotaKey.of(callerId, group);
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
                    if (quotaManager.isPaused(key)) {
                        // 暂停派发：任务保持 QUEUED/PENDING_RETRY 不丢失、不拒绝，
                        // 等恢复后按原顺序继续。
                        blocked.put(key, r.getTaskId());
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
