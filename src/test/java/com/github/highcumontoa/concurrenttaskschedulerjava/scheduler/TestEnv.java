package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler;

import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.config.SchedulerProperties;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.TaskRecord;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.TaskStatus;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler.DefaultTaskHandlerRegistry;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler.TaskHandler;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.quota.InMemoryQuotaManager;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.service.DefaultTaskSchedulerService;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.service.TaskSchedulerService;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.store.FileTaskStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.awaitility.Awaitility.await;

/**
 * 测试环境：每个用例独立的数据目录与调度器实例，避免相互干扰。
 */
public final class TestEnv implements AutoCloseable {

    public final Path dataDir;
    public final SchedulerProperties props;
    public final DefaultTaskHandlerRegistry registry;
    public final DefaultTaskSchedulerService service;

    private final List<LatchHandler> handlers = new ArrayList<>();

    private TestEnv(Path dataDir, SchedulerProperties props) {
        this.dataDir = dataDir;
        this.props = props;
        this.registry = new DefaultTaskHandlerRegistry(List.of());
        FileTaskStore store = new FileTaskStore(dataDir.toString());
        InMemoryQuotaManager quota = new InMemoryQuotaManager(props);
        this.service = new DefaultTaskSchedulerService(store, quota, registry, props, Clock.systemUTC());
        this.service.start();
    }

    public static SchedulerProperties defaultProps() {
        SchedulerProperties p = new SchedulerProperties();
        p.setTickMillis(5);
        p.setAttemptTimeoutMillis(500);
        p.setBackoffBaseMillis(20);
        p.setBackoffMultiplier(2.0);
        p.setMaxBackoffMillis(200);
        p.setDefaultMaxAttempts(3);
        return p;
    }

    public static TestEnv create(SchedulerProperties props) {
        try {
            Path dir = Files.createTempDirectory("scheduler-test-");
            props.setDataDir(dir.toString());
            return new TestEnv(dir, props);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** 复用既有数据目录重新启动一个调度器实例（模拟进程重启）。 */
    public static TestEnv open(SchedulerProperties props, Path dataDir) {
        props.setDataDir(dataDir.toString());
        return new TestEnv(dataDir, props);
    }

    /** 注册一个可手动放行、可统计进入次数、可感知取消的阻塞处理器。 */
    public LatchHandler registerLatch(String type) {
        LatchHandler h = new LatchHandler(type);
        registry.register(h);
        handlers.add(h);
        return h;
    }

    public void register(TaskHandler h) {
        registry.register(h);
    }

    public void awaitStatus(String taskId, Predicate<String> statusCheck, long millis) {
        await().atMost(Duration.ofMillis(millis))
                .pollInterval(Duration.ofMillis(5))
                .until(() -> {
                    var r = service.get(taskId);
                    return r != null && statusCheck.test(r.getStatus().name());
                });
    }

    @Override
    public void close() {
        service.stop();
    }

    /**
     * 直接在磁盘上植入一条处于 RUNNING 的任务记录（不经调度器），
     * 等价于「进程在任务执行中被 kill -9」后的磁盘状态。
     */
    public String seedRunningTask(String caller, String submitKey, String taskType, int attemptCount) {
        TaskRecord r = TaskRecord.create(
                java.util.UUID.randomUUID().toString().replace("-", ""),
                caller, submitKey, caller, taskType, "seed", 3, System.currentTimeMillis());
        r.setStatus(TaskStatus.RUNNING);
        r.setStartedAt(System.currentTimeMillis());
        r.setAttemptCount(attemptCount);
        new com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.store.FileTaskStore(
                dataDir.toString()).putIfAbsent(r);
        return r.getTaskId();
    }

    /**
     * 模拟进程被强制杀死：立即关闭线程池且不等待任务收尾，
     * 运行中的任务记录停留在 RUNNING（工作线程没有机会落盘终态）。
     */
    public void kill() {
        service.killForRestartTest();
    }
}
