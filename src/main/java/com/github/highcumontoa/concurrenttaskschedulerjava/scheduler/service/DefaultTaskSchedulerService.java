package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.service;

import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.config.SchedulerProperties;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.AttemptInfo;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.FailureCode;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.FailureInfo;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.QuotaSnapshot;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.QuotaVerdict;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.RejectReason;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.RetryPolicy;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.SubmitResult;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.TaskRecord;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.TaskStatus;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler.NonRetryableTaskException;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler.TaskContext;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler.TaskHandler;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler.TaskHandlerRegistry;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.quota.QuotaManager;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.store.TaskStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 默认调度服务：本地、无外部依赖的配额感知异步任务调度器。
 *
 * <h3>一致性约定</h3>
 * <ul>
 *   <li>所有状态变化「先持久化后生效」：{@link TaskStore#save(TaskRecord)} 成功后才释放线程/返回；</li>
 *   <li>重复提交由存储层 (caller, submitKey) 唯一索引保证，命中即返回原任务（不重复执行、不重复占配额）；</li>
 *   <li>配额的判定与记账在 {@link QuotaManager} 内原子完成，状态迁移在 {@link #stateLock} 内完成；</li>
 *   <li>重启后：QUEUED/PENDING 重新排队；RUNNING 默认标记 FAILED(RESTART_INTERRUPTED)，
 *       配置 retryRunningTasksOnRestart=true 时退避重试一次（要求处理器幂等）；
 *       WAITING_RETRY 按 nextRunAt 继续等待；运行计数归零（不信任崩溃进程的槽位，杜绝配额泄漏）。</li>
 * </ul>
 */
public class DefaultTaskSchedulerService implements TaskSchedulerService {

    private static final Logger log = LoggerFactory.getLogger(DefaultTaskSchedulerService.class);
    private static final int MAX_MESSAGE_LEN = 2000;

    /** 延迟队列条目：readyAt 为可派发时间（排队任务=queuedAt，重试任务=nextRunAt）。 */
    private static final class ReadyItem implements Delayed {
        final String taskId;
        final long readyAt;

        ReadyItem(String taskId, long readyAt) {
            this.taskId = taskId;
            this.readyAt = readyAt;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(readyAt - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return Long.compare(readyAt, ((ReadyItem) o).readyAt);
        }
    }

    /** 单次执行的可变控制位（取消信号、超时信号、执行线程）。 */
    private static final class Execution {
        final AtomicBoolean cancelRequested = new AtomicBoolean(false);
        final AtomicBoolean timedOut = new AtomicBoolean(false);
        volatile Thread workerThread;
    }

    /** 任务上下文实现。 */
    private static final class ContextImpl implements TaskContext {
        private final String taskId;
        private final String caller;
        private final Execution execution;

        ContextImpl(String taskId, String caller, Execution execution) {
            this.taskId = taskId;
            this.caller = caller;
            this.execution = execution;
        }

        @Override public String getTaskId() { return taskId; }
        @Override public String getCaller() { return caller; }
        @Override public boolean isCancelled() {
            return execution.cancelRequested.get() || Thread.currentThread().isInterrupted();
        }
    }

    private final TaskStore store;
    private final QuotaManager quotaManager;
    private final TaskHandlerRegistry handlerRegistry;
    private final SchedulerProperties props;
    private final Clock clock;

    private final Object stateLock = new Object();
    private final Map<String, TaskRecord> cache = new HashMap<>();
    private final Map<String, Execution> executions = new HashMap<>();

    private final java.util.concurrent.DelayQueue<ReadyItem> readyQueue =
            new java.util.concurrent.DelayQueue<>();

    private ScheduledExecutorService dispatcher;
    private ExecutorService workers;
    private volatile boolean running;

    public DefaultTaskSchedulerService(TaskStore store,
                                       QuotaManager quotaManager,
                                       TaskHandlerRegistry handlerRegistry,
                                       SchedulerProperties props,
                                       Clock clock) {
        this.store = store;
        this.quotaManager = quotaManager;
        this.handlerRegistry = handlerRegistry;
        this.props = props;
        this.clock = clock;
    }

    private long now() {
        return clock.millis();
    }

    private RetryPolicy policyFor(String caller) {
        int maxAttempts = props.getDefaultMaxAttempts();
        SchedulerProperties.CallerQuota c = props.getCallers().get(caller);
        if (c != null && c.getMaxAttempts() != null) {
            maxAttempts = c.getMaxAttempts();
        }
        return new RetryPolicy(maxAttempts, props.getBackoffBaseMillis(),
                props.getBackoffMultiplier(), props.getMaxBackoffMillis());
    }

    private static String clip(String message) {
        if (message == null) {
            return "";
        }
        return message.length() <= MAX_MESSAGE_LEN ? message : message.substring(0, MAX_MESSAGE_LEN) + "...[truncated]";
    }

    // ==================== 生命周期 ====================

    @Override
    public void start() {
        synchronized (stateLock) {
            if (running) {
                return;
            }
            recover();
            ThreadFactory tf = r -> {
                Thread t = new Thread(r, "task-worker");
                t.setDaemon(true);
                return t;
            };
            workers = Executors.newFixedThreadPool(Math.max(1, props.getGlobalMaxConcurrency()), tf);

            dispatcher = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "task-dispatcher");
                t.setDaemon(true);
                return t;
            });
            long tick = Math.max(1, props.getTickMillis());
            dispatcher.scheduleWithFixedDelay(this::dispatchTick, tick, tick, TimeUnit.MILLISECONDS);
            running = true;
            log.info("调度器已启动: tick={}ms workers={}", tick, props.getGlobalMaxConcurrency());
        }
    }

    @Override
    public void stop() {
        synchronized (stateLock) {
            if (!running) {
                return;
            }
            running = false;
        }
        dispatcher.shutdownNow();
        workers.shutdown();
        try {
            if (!workers.awaitTermination(5, TimeUnit.SECONDS)) {
                workers.shutdownNow();
            }
        } catch (InterruptedException e) {
            workers.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("调度器已停止（未完成任务保留在磁盘，重启后恢复）");
    }

    /**
     * 仅供测试：模拟进程被杀死。立即关闭线程池，不等待运行中的任务收尾，
     * 因此任务记录停留在 RUNNING/QUEUED 等磁盘状态，供重启恢复验证。
     */
    public void killForRestartTest() {
        synchronized (stateLock) {
            running = false;
        }
        if (dispatcher != null) {
            dispatcher.shutdownNow();
        }
        if (workers != null) {
            workers.shutdownNow();
        }
    }

    /** 启动恢复：状态修正 + 配额计数重建。 */
    private void recover() {
        cache.clear();
        readyQueue.clear();
        executions.clear();

        int globalQueued = 0;
        Map<String, Integer> queuedByCaller = new HashMap<>();

        for (TaskRecord loaded : store.loadAll()) {
            TaskRecord r = loaded.copy();
            long t = now();
            switch (r.getStatus()) {
                case PENDING, QUEUED -> {
                    r.setStatus(TaskStatus.QUEUED);
                    if (r.getQueuedAt() == 0) {
                        r.setQueuedAt(r.getUpdatedAt());
                    }
                    r.setUpdatedAt(t);
                    store.save(r);
                    readyQueue.add(new ReadyItem(r.getTaskId(), r.getQueuedAt()));
                    globalQueued++;
                    queuedByCaller.merge(r.getCaller(), 1, Integer::sum);
                    log.info("恢复任务-重新排队: taskId={} caller={} submitKey={}",
                            r.getTaskId(), r.getCaller(), r.getSubmitKey());
                }
                case WAITING_RETRY -> {
                    long readyAt = Math.max(r.getNextRunAt(), r.getUpdatedAt());
                    readyQueue.add(new ReadyItem(r.getTaskId(), readyAt));
                    log.info("恢复任务-继续等待重试: taskId={} caller={} nextRunAt={} attempt={}/{}",
                            r.getTaskId(), r.getCaller(), r.getNextRunAt(),
                            r.getAttemptCount(), r.getMaxAttempts());
                }
                case RUNNING -> {
                    if (props.isRetryRunningTasksOnRestart() && r.getAttemptCount() < r.getMaxAttempts()) {
                        r.setStatus(TaskStatus.WAITING_RETRY);
                        long delay = policyFor(r.getCaller()).backoffMillis(r.getAttemptCount());
                        r.setNextRunAt(t + delay);
                        r.setFailure(new FailureInfo(FailureCode.RESTART_INTERRUPTED, true,
                                "进程重启中断执行，将退避重试（要求处理器幂等）", t));
                        r.setUpdatedAt(t);
                        store.save(r);
                        readyQueue.add(new ReadyItem(r.getTaskId(), r.getNextRunAt()));
                        log.info("恢复任务-中断后重试: taskId={} caller={} nextRunAt={}",
                                r.getTaskId(), r.getCaller(), r.getNextRunAt());
                    } else {
                        r.setStatus(TaskStatus.FAILED);
                        r.setFailure(new FailureInfo(FailureCode.RESTART_INTERRUPTED, false,
                                "进程重启中断执行，默认不自动重试（scheduler.retry-running-tasks-on-restart 可开启）", t));
                        r.setFinishedAt(t);
                        r.setUpdatedAt(t);
                        store.save(r);
                        log.info("恢复任务-中断判失败: taskId={} caller={}", r.getTaskId(), r.getCaller());
                    }
                }
                default -> {
                    // 终态任务：不计数、不排队
                    if (!r.getStatus().isTerminal()) {
                        log.warn("恢复时遇到未知状态，按失败处理: taskId={} status={}",
                                r.getTaskId(), r.getStatus());
                        r.setStatus(TaskStatus.FAILED);
                        store.save(r);
                    }
                }
            }
            cache.put(r.getTaskId(), r);
        }
        // 崩溃进程的运行槽位一律不再信任，运行计数从 0 开始
        quotaManager.recover(0, Map.of(), globalQueued, queuedByCaller);
        log.info("恢复完成: loaded={} queued={}", cache.size(), globalQueued);
    }

    // ==================== 提交 ====================

    @Override
    public SubmitResult submit(String caller, String submitKey, String taskType, String payload) {
        if (caller == null || caller.isBlank()) {
            throw new IllegalArgumentException("caller 不能为空");
        }
        if (taskType == null || taskType.isBlank()) {
            throw new IllegalArgumentException("taskType 不能为空");
        }
        if (handlerRegistry.find(taskType).isEmpty()) {
            throw new IllegalArgumentException("未知 taskType: " + taskType);
        }
        RetryPolicy policy = policyFor(caller);
        String taskId = UUID.randomUUID().toString().replace("-", "");
        TaskRecord fresh = TaskRecord.create(taskId, caller,
                submitKey == null || submitKey.isBlank() ? null : submitKey,
                caller, taskType, payload, policy.getMaxAttempts(), now());

        // 1) 持久化 + 去重（存储唯一索引保证并发安全）
        TaskRecord existing = store.putIfAbsent(fresh);
        if (existing != null) {
            TaskRecord toRun = null;
            SubmitResult result;
            synchronized (stateLock) {
                TaskRecord current = cache.get(existing.getTaskId());
                TaskRecord rec = current != null ? current : existing.copy();
                if (current == null) {
                    cache.put(rec.getTaskId(), rec);
                }
                if (rec.getStatus() == TaskStatus.PENDING) {
                    // 罕见竞态：首次提交刚写完 PENDING 尚未完成配额判定。
                    // 由当前 duplicate 请求接手完成判定与派发，保证任务不会丢失。
                    result = finishAdmission(caller, rec, true);
                    if (rec.getStatus() == TaskStatus.RUNNING) {
                        toRun = rec.copy();
                    }
                } else {
                    QuotaVerdict v = switch (rec.getStatus()) {
                        case REJECTED -> QuotaVerdict.REJECT;
                        case QUEUED, WAITING_RETRY -> QuotaVerdict.ENQUEUE;
                        default -> QuotaVerdict.START;
                    };
                    result = new SubmitResult(rec.copy(), true, v, null);
                }
            }
            log.info("提交判定: caller={} submitKey={} taskId={} verdict=DUPLICATE status={}",
                    caller, submitKey, existing.getTaskId(), result.getRecord().getStatus());
            if (toRun != null) {
                launchAttempt(toRun);
            }
            return result;
        }

        // 2) 配额判定 + 状态落盘（状态锁内，保证计数与记录一致）
        TaskRecord toRun;
        SubmitResult result;
        synchronized (stateLock) {
            cache.put(taskId, fresh);
            result = finishAdmission(caller, fresh, false);
            toRun = fresh.getStatus() == TaskStatus.RUNNING ? fresh.copy() : null;
        }

        // 3) 立即启动的任务交工作线程（锁外执行，避免持锁跑业务）
        if (toRun != null) {
            launchAttempt(toRun);
        }
        return result;
    }

    /**
     * 在状态锁内完成一条新记录的准入判定：立即启动 / 排队 / 拒绝，并同步落盘。
     * 调用前记录必须已是 cache 中的 PENDING 状态。
     */
    private SubmitResult finishAdmission(String caller, TaskRecord fresh, boolean duplicate) {
        long t = now();
        if (quotaManager.tryAcquireRunning(caller)) {
            fresh.setStatus(TaskStatus.RUNNING);
            fresh.setStartedAt(t);
            fresh.setQueuedAt(0);
            fresh.setAttemptCount(1);
            fresh.setUpdatedAt(t);
            store.save(fresh);
            log.info("提交判定: caller={} submitKey={} taskId={} verdict=START{}",
                    caller, fresh.getSubmitKey(), fresh.getTaskId(), duplicate ? "(接手duplicate)" : "");
            return new SubmitResult(fresh.copy(), duplicate, QuotaVerdict.START, null);
        } else if (quotaManager.tryAcquireQueued(caller)) {
            fresh.setStatus(TaskStatus.QUEUED);
            fresh.setQueuedAt(t);
            fresh.setUpdatedAt(t);
            store.save(fresh);
            readyQueue.add(new ReadyItem(fresh.getTaskId(), fresh.getQueuedAt()));
            log.info("提交判定: caller={} submitKey={} taskId={} verdict=ENQUEUE{}",
                    caller, fresh.getSubmitKey(), fresh.getTaskId(), duplicate ? "(接手duplicate)" : "");
            return new SubmitResult(fresh.copy(), duplicate, QuotaVerdict.ENQUEUE, null);
        } else {
            RejectReason reason = diagnoseReject(caller);
            FailureCode code = reason == RejectReason.CALLER_QUEUE_FULL
                    ? FailureCode.CALLER_QUEUE_FULL : FailureCode.GLOBAL_QUEUE_FULL;
            fresh.setStatus(TaskStatus.REJECTED);
            fresh.setFailure(new FailureInfo(code, false,
                    "配额不足被拒绝: " + reason, t));
            fresh.setFinishedAt(t);
            fresh.setUpdatedAt(t);
            store.save(fresh);
            log.info("提交判定: caller={} submitKey={} taskId={} verdict=REJECT reason={}{}",
                    caller, fresh.getSubmitKey(), fresh.getTaskId(), reason,
                    duplicate ? "(接手duplicate)" : "");
            return new SubmitResult(fresh.copy(), duplicate, QuotaVerdict.REJECT, reason);
        }
    }

    private RejectReason diagnoseReject(String caller) {
        // 与 InMemoryQuotaManager 的判定顺序保持一致：调用方队列优先，其次全局
        QuotaSnapshot cs = quotaManager.callerSnapshot(caller);
        if (cs.getQueued() >= cs.getMaxQueued()) {
            return RejectReason.CALLER_QUEUE_FULL;
        }
        return RejectReason.GLOBAL_QUEUE_FULL;
    }

    // ==================== 派发 ====================

    private void dispatchTick() {
        try {
            long tickDeadline = now() + Math.max(1, props.getTickMillis()) / 2;
            java.util.List<ReadyItem> blocked = new java.util.ArrayList<>();
            ReadyItem item;
            while ((item = readyQueue.poll()) != null) {
                TaskRecord toRun = tryDispatch(item);
                if (toRun == null) {
                    blocked.add(item);
                } else {
                    launchAttempt(toRun);
                }
                if (now() > tickDeadline) {
                    break;
                }
            }
            // 未到期（理论上 DelayQueue.poll 不会取出）或配额暂不可用的条目放回，等待后续 tick
            readyQueue.addAll(blocked);
        } catch (Throwable t) {
            log.error("派发循环异常（下一 tick 自动恢复）", t);
        }
    }

    /** 尝试派发单个条目；返回需要立即执行的任务副本，失败/失效返回 null。 */
    private TaskRecord tryDispatch(ReadyItem item) {
        synchronized (stateLock) {
            if (!running) {
                return null;
            }
            TaskRecord r = cache.get(item.taskId);
            if (r == null) {
                return null;
            }
            if (r.getStatus() != TaskStatus.QUEUED && r.getStatus() != TaskStatus.WAITING_RETRY) {
                return null; // 已取消/已被其他路径处理的陈旧条目
            }
            if (r.getStatus() == TaskStatus.WAITING_RETRY && now() < r.getNextRunAt()) {
                return null;
            }
            if (!quotaManager.tryAcquireRunning(r.getCaller())) {
                return null; // 并发槽位或速率令牌不足，下个 tick 重试
            }
            boolean fromQueue = r.getStatus() == TaskStatus.QUEUED;
            if (fromQueue) {
                quotaManager.releaseQueued(r.getCaller());
            }
            r.setStatus(TaskStatus.RUNNING);
            r.setStartedAt(now());
            r.setAttemptCount(r.getAttemptCount() + 1);
            r.setFailure(null);
            r.setUpdatedAt(now());
            store.save(r);
            log.info("状态变化: taskId={} caller={} submitKey={} {} -> RUNNING attempt={}/{}",
                    r.getTaskId(), r.getCaller(), r.getSubmitKey(),
                    fromQueue ? "QUEUED" : "WAITING_RETRY", r.getAttemptCount(), r.getMaxAttempts());
            return r.copy();
        }
    }

    // ==================== 执行 ====================

    private void launchAttempt(TaskRecord snapshot) {
        Execution execution = new Execution();
        synchronized (stateLock) {
            executions.put(snapshot.getTaskId(), execution);
        }
        workers.submit(() -> runAttempt(snapshot, execution));
    }

    private void runAttempt(TaskRecord snapshot, Execution execution) {
        String taskId = snapshot.getTaskId();
        String caller = snapshot.getCaller();
        execution.workerThread = Thread.currentThread();

        TaskHandler handler = handlerRegistry.find(snapshot.getTaskType()).orElse(null);
        ContextImpl context = new ContextImpl(taskId, caller, execution);

        // 超时看门狗：到点中断工作线程并终态化 TIMED_OUT
        ScheduledExecutorService watchdog = dispatcher;
        final Future<?> timeoutFuture;
        if (props.getAttemptTimeoutMillis() > 0 && watchdog != null) {
            timeoutFuture = watchdog.schedule(() -> onAttemptTimeout(taskId, execution),
                    props.getAttemptTimeoutMillis(), TimeUnit.MILLISECONDS);
        } else {
            timeoutFuture = null;
        }

        long start = now();
        Throwable error = null;
        boolean success = false;
        try {
            if (handler == null) {
                throw new IllegalStateException("无对应处理器: " + snapshot.getTaskType());
            }
            handler.handle(snapshot.getPayload(), context);
            success = true;
        } catch (NonRetryableTaskException e) {
            error = e;
        } catch (Throwable t) {
            error = t;
        } finally {
            if (timeoutFuture != null) {
                timeoutFuture.cancel(false);
            }
        }
        long finish = now();

        if (execution.timedOut.get()) {
            // 超时路径已在 onAttemptTimeout 中终态化并释放配额
            log.info("尝试结束: taskId={} attempt={} result=TIMED_OUT(已终态化)",
                    taskId, snapshot.getAttemptCount());
            cleanupExecution(taskId, execution);
            return;
        }
        if (success) {
            finishAttemptSuccess(taskId, caller, snapshot.getAttemptCount(), start, finish, execution);
        } else {
            boolean nonRetryable = error instanceof NonRetryableTaskException;
            finishAttemptFailure(taskId, caller, snapshot.getAttemptCount(), start, finish,
                    nonRetryable ? FailureCode.NON_RETRYABLE : FailureCode.HANDLER_EXCEPTION,
                    nonRetryable, error, execution);
        }
    }

    private void onAttemptTimeout(String taskId, Execution execution) {
        execution.timedOut.set(true);
        Thread t = execution.workerThread;
        if (t != null) {
            t.interrupt();
        }
        boolean terminalWon;
        synchronized (stateLock) {
            TaskRecord r = cache.get(taskId);
            terminalWon = transitionToTerminalLocked(r, TaskStatus.TIMED_OUT,
                    new FailureInfo(FailureCode.ATTEMPT_TIMEOUT, false,
                            "单次执行超过 " + props.getAttemptTimeoutMillis() + "ms，任务终止（不重试）", now()));
            if (terminalWon) {
                r.getAttempts().add(new AttemptInfo(r.getAttemptCount(),
                        r.getStartedAt(), now(), false, FailureCode.ATTEMPT_TIMEOUT,
                        "执行超时，工作线程已请求中断"));
                r.setFinishedAt(now());
                store.save(r);
                quotaManager.releaseRunning(r.getCaller());
            }
        }
        if (terminalWon) {
            log.warn("状态变化: taskId={} RUNNING -> TIMED_OUT timeoutMs={}",
                    taskId, props.getAttemptTimeoutMillis());
        }
        cleanupExecution(taskId, execution);
    }

    private void finishAttemptSuccess(String taskId, String caller, int attemptNo,
                                      long start, long finish, Execution execution) {
        synchronized (stateLock) {
            TaskRecord r = cache.get(taskId);
            if (r == null || r.getStatus() != TaskStatus.RUNNING) {
                return; // 取消/超时已赢得终态
            }
            r.getAttempts().add(new AttemptInfo(attemptNo, start, finish, true, null, null));
            r.setStatus(TaskStatus.COMPLETED);
            r.setFailure(null);
            r.setFinishedAt(finish);
            r.setUpdatedAt(finish);
            store.save(r);
            quotaManager.releaseRunning(caller);
            log.info("状态变化: taskId={} caller={} submitKey={} RUNNING -> COMPLETED attempt={}",
                    taskId, caller, r.getSubmitKey(), attemptNo);
        }
        cleanupExecution(taskId, execution);
    }

    private void finishAttemptFailure(String taskId, String caller, int attemptNo,
                                      long start, long finish, FailureCode code,
                                      boolean nonRetryable, Throwable error, Execution execution) {
        synchronized (stateLock) {
            TaskRecord r = cache.get(taskId);
            if (r == null || r.getStatus() != TaskStatus.RUNNING) {
                return; // 取消/超时已赢得终态
            }
            boolean cancelled = execution.cancelRequested.get()
                    || error instanceof InterruptedException
                    || Thread.currentThread().isInterrupted();

            if (cancelled) {
                r.getAttempts().add(new AttemptInfo(attemptNo, start, finish, false,
                        FailureCode.CANCELLED, clip(safeMessage(error))));
                r.setStatus(TaskStatus.CANCELLED);
                r.setFailure(new FailureInfo(FailureCode.CANCELLED, false,
                        "任务被调用方取消", now()));
                r.setFinishedAt(now());
                r.setUpdatedAt(now());
                store.save(r);
                quotaManager.releaseRunning(caller);
                log.info("状态变化: taskId={} caller={} submitKey={} RUNNING -> CANCELLED attempt={}",
                        taskId, caller, r.getSubmitKey(), attemptNo);
                cleanupExecution(taskId, execution);
                return;
            }

            r.getAttempts().add(new AttemptInfo(attemptNo, start, finish, false,
                    code, clip(safeMessage(error))));

            RetryPolicy policy = policyFor(caller);
            boolean canRetry = !nonRetryable && attemptNo < r.getMaxAttempts();
            if (canRetry) {
                long delay = policy.backoffMillis(attemptNo);
                r.setStatus(TaskStatus.WAITING_RETRY);
                r.setNextRunAt(now() + delay);
                r.setFailure(new FailureInfo(code, true,
                        clip(safeMessage(error)) + " | 将进行第 " + (attemptNo + 1) + " 次尝试，退避 "
                                + delay + "ms", now()));
                r.setUpdatedAt(now());
                store.save(r);
                quotaManager.releaseRunning(caller);
                readyQueue.add(new ReadyItem(taskId, r.getNextRunAt()));
                log.info("状态变化: taskId={} caller={} submitKey={} RUNNING -> WAITING_RETRY attempt={}/{} backoff={}ms code={}",
                        taskId, caller, r.getSubmitKey(), attemptNo, r.getMaxAttempts(), delay, code);
            } else {
                r.setStatus(TaskStatus.FAILED);
                String reason = nonRetryable
                        ? "不可重试错误，任务终止: " + clip(safeMessage(error))
                        : "已达最大尝试次数 " + r.getMaxAttempts() + "，任务终止: " + clip(safeMessage(error));
                r.setFailure(new FailureInfo(nonRetryable ? FailureCode.NON_RETRYABLE : FailureCode.HANDLER_EXCEPTION,
                        false, reason, now()));
                r.setFinishedAt(now());
                r.setUpdatedAt(now());
                store.save(r);
                quotaManager.releaseRunning(caller);
                log.info("状态变化: taskId={} caller={} submitKey={} RUNNING -> FAILED attempt={}/{} code={} reason={}",
                        taskId, caller, r.getSubmitKey(), attemptNo, r.getMaxAttempts(),
                        nonRetryable ? FailureCode.NON_RETRYABLE : FailureCode.HANDLER_EXCEPTION,
                        clip(safeMessage(error)));
            }
        }
        cleanupExecution(taskId, execution);
    }

    private static String safeMessage(Throwable t) {
        if (t == null) {
            return "";
        }
        return t.getClass().getSimpleName() + ": " + t.getMessage();
    }

    /** 仅当任务仍处于 RUNNING 时迁移到指定终态（CAS 语义）。 */
    private boolean transitionToTerminalLocked(TaskRecord r, TaskStatus target, FailureInfo info) {
        if (r == null || r.getStatus() != TaskStatus.RUNNING) {
            return false;
        }
        r.setStatus(target);
        r.setFailure(info);
        r.setFinishedAt(now());
        r.setUpdatedAt(now());
        return true;
    }

    private void cleanupExecution(String taskId, Execution execution) {
        synchronized (stateLock) {
            if (executions.get(taskId) == execution) {
                executions.remove(taskId);
            }
        }
        // 清除中断标记，避免线程池复用线程时影响下一个任务
        Thread.interrupted();
    }

    // ==================== 查询 / 取消 ====================

    @Override
    public TaskRecord get(String taskId) {
        synchronized (stateLock) {
            TaskRecord r = cache.get(taskId);
            if (r != null) {
                return r.copy();
            }
        }
        TaskRecord loaded = store.findById(taskId);
        return loaded == null ? null : loaded.copy();
    }

    @Override
    public TaskRecord getBySubmitKey(String caller, String submitKey) {
        TaskRecord loaded = store.findBySubmitKey(caller, submitKey);
        return loaded == null ? null : loaded.copy();
    }

    @Override
    public TaskRecord cancel(String taskId) {
        synchronized (stateLock) {
            TaskRecord r = cache.get(taskId);
            if (r == null) {
                r = store.findById(taskId);
                if (r == null) {
                    return null;
                }
                cache.put(taskId, r);
            }
            if (r.getStatus().isTerminal()) {
                log.info("取消请求: taskId={} 已是终态 status={}", taskId, r.getStatus());
                return r.copy();
            }
            switch (r.getStatus()) {
                case QUEUED, PENDING -> {
                    r.setStatus(TaskStatus.CANCELLED);
                    r.setFailure(new FailureInfo(FailureCode.CANCELLED, false,
                            "排队中被调用方取消，未执行", now()));
                    r.setFinishedAt(now());
                    r.setUpdatedAt(now());
                    store.save(r);
                    quotaManager.releaseQueued(r.getCaller());
                    log.info("状态变化: taskId={} caller={} submitKey={} QUEUED -> CANCELLED（未执行，队列名额已释放）",
                            taskId, r.getCaller(), r.getSubmitKey());
                    return r.copy();
                }
                case WAITING_RETRY -> {
                    // 重试等待不占队列/运行槽位，直接终态化，陈旧 ReadyItem 会在弹出时丢弃
                    r.setStatus(TaskStatus.CANCELLED);
                    r.setFailure(new FailureInfo(FailureCode.CANCELLED, false,
                            "重试等待中被调用方取消", now()));
                    r.setFinishedAt(now());
                    r.setUpdatedAt(now());
                    store.save(r);
                    log.info("状态变化: taskId={} caller={} submitKey={} WAITING_RETRY -> CANCELLED",
                            taskId, r.getCaller(), r.getSubmitKey());
                    return r.copy();
                }
                case RUNNING -> {
                    Execution execution = executions.get(taskId);
                    if (execution != null) {
                        execution.cancelRequested.set(true);
                        if (execution.workerThread != null) {
                            execution.workerThread.interrupt();
                        }
                    }
                    log.info("取消请求: taskId={} caller={} submitKey={} 运行中，已发出中断，等待协作式停止",
                            taskId, r.getCaller(), r.getSubmitKey());
                    return r.copy();
                }
                default -> {
                    return r.copy();
                }
            }
        }
    }

    @Override
    public QuotaSnapshot quota(String caller) {
        if (caller == null || caller.isBlank()) {
            return quotaManager.globalSnapshot();
        }
        return quotaManager.callerSnapshot(caller);
    }
}
