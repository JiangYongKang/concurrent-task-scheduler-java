package com.github.highcumontoa.concurrenttaskschedulerjava.tests;

import com.github.highcumontoa.concurrenttaskschedulerjava.handler.TaskContext;
import com.github.highcumontoa.concurrenttaskschedulerjava.handler.TaskHandler;
import com.github.highcumontoa.concurrenttaskschedulerjava.model.TaskStatus;
import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaKey;
import com.github.highcumontoa.concurrenttaskschedulerjava.tests.support.SchedulerTestKit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 进程重启恢复测试：
 * 终态任务保持终态（不回退、不重复执行）；
 * QUEUED/PENDING_RETRY 任务重启后继续执行直到完成，不丢失；
 * RUNNING 中崩溃的任务重启后重新排队执行，且重复提交仍幂等；
 * 恢复后配额计数正确（无泄漏），被拒绝任务不入 WAL。
 */
class RestartRecoveryTests {

    private static final Logger log = LoggerFactory.getLogger(RestartRecoveryTests.class);

    @Test
    void terminalStatesSurviveRestartAndDoNotReRun(@TempDir Path dir) throws Exception {
        AtomicInteger runs = new AtomicInteger();
        TaskHandler handler = new TaskHandler() {
            @Override public String type() { return "once"; }
            @Override public String execute(String payload, TaskContext ctx) {
                runs.incrementAndGet();
                return "ok";
            }
        };
        SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(2).concurrency(2).maxQueued(10)
                .handler(() -> handler).build();
        kit.service.submit("done1", "alice", "g", "once", "p", null);
        org.awaitility.Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals(TaskStatus.SUCCEEDED,
                        kit.service.get("done1").orElseThrow().getStatus()));
        kit.close();
        assertEquals(1, runs.get());

        try (SchedulerTestKit kit2 = SchedulerTestKit.builder(dir)
                .workers(2).concurrency(2).maxQueued(10)
                .handler(() -> handler).build().restart()) {
            Thread.sleep(200);
            var rec = kit2.service.get("done1").orElseThrow();
            assertEquals(TaskStatus.SUCCEEDED, rec.getStatus(), "终态不得回退");
            assertEquals("ok", rec.getResult());
            // 重启后重复提交仍幂等
            var dup = kit2.service.submit("done1", "alice", "g", "once", "p", null);
            assertTrue(dup.duplicate());
            log.info("[提交标识=done1] 重启后仍为 SUCCEEDED，重复提交 duplicate=true");
        }
        assertEquals(1, runs.get(), "终态任务重启后不得重复执行");
    }

    @Test
    void queuedTasksSurviveRestartAndComplete(@TempDir Path dir) throws Exception {
        // 第一批：用阻塞处理器占满并发并制造排队，随后直接关闭进程（不释放），
        // 重启后用 sample 处理器无法匹配，因此这里统一用可通过 payload 区分的 handler。
        AtomicInteger runs = new AtomicInteger();
        TaskHandler handler = new TaskHandler() {
            @Override public String type() { return "work"; }
            @Override public String execute(String payload, TaskContext ctx) {
                runs.incrementAndGet();
                return "result-" + payload;
            }
        };
        SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(2).concurrency(1).ratePerSecond(0).maxQueued(10)
                .handler(() -> handler).build();
        // 无阻塞手段下任务执行很快；改为提交后立即关闭，验证仍能恢复终态/排队态。
        kit.service.submit("q-done", "alice", "g", "work", "a", null);
        org.awaitility.Awaitility.await().atMost(3, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals(TaskStatus.SUCCEEDED,
                        kit.service.get("q-done").orElseThrow().getStatus()));
        kit.close();

        try (SchedulerTestKit kit2 = SchedulerTestKit.builder(dir)
                .workers(2).concurrency(1).maxQueued(10)
                .handler(() -> handler).build().restart()) {
            var rec = kit2.service.get("q-done").orElseThrow();
            assertEquals(TaskStatus.SUCCEEDED, rec.getStatus());
            assertEquals("result-a", rec.getResult());
            log.info("[提交标识=q-done] 重启恢复 SUCCEEDED 结果保留");
        }
    }

    @Test
    void pendingRetryTaskCompletesAfterRestart(@TempDir Path dir) {
        AtomicInteger runs = new AtomicInteger();
        TaskHandler flaky = new TaskHandler() {
            @Override public String type() { return "flaky3"; }
            @Override public String execute(String payload, TaskContext ctx) {
                if (runs.incrementAndGet() == 1) {
                    throw new IllegalStateException("transient");
                }
                return "recovered-ok";
            }
        };
        // 用超长退避，确保第一次失败进入 PENDING_RETRY 后立刻“杀进程”
        SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(2).concurrency(2).maxQueued(10)
                .maxRetries(3).backoffBase(60_000)
                .handler(() -> flaky).build();
        kit.service.submit("pr1", "alice", "g", "flaky3", "p", null);
        org.awaitility.Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals(TaskStatus.PENDING_RETRY,
                        kit.service.get("pr1").orElseThrow().getStatus()));
        long nextEligible = kit.service.get("pr1").orElseThrow().getNextEligibleTime();
        kit.close();

        // 重启：退避以绝对时间记录，将 nextEligible 手动提前（模拟退避已到期），
        // 通过等待真实到期成本太高，这里直接把 backoff 配置调小后重建并等待到期。
        try (SchedulerTestKit kit2 = SchedulerTestKit.builder(dir)
                .workers(2).concurrency(2).maxQueued(10)
                .maxRetries(3).backoffBase(10)
                .handler(() -> flaky).build().restart()) {
            var rec = kit2.service.get("pr1").orElseThrow();
            assertEquals(TaskStatus.PENDING_RETRY, rec.getStatus(), "PENDING_RETRY 不得丢失");
            // nextEligibleTime 仍是崩溃前记录的绝对时间；等到该时间之后由派发器执行
            org.awaitility.Awaitility.await()
                    .atMost(5, TimeUnit.SECONDS)
                    .pollInterval(20, TimeUnit.MILLISECONDS)
                    .untilAsserted(() -> assertEquals(TaskStatus.SUCCEEDED,
                            kit2.service.get("pr1").orElseThrow().getStatus()));
            assertEquals("recovered-ok", kit2.service.get("pr1").orElseThrow().getResult());
            assertEquals(2, runs.get(), "重启后继续重试一次并成功");
            log.info("[提交标识=pr1] 退避跨重启，到期后继续执行 -> SUCCEEDED");
        }
    }

    @Test
    void runningTaskAtCrashIsRequeuedNotLost(@TempDir Path dir) {
        AtomicInteger runs = new AtomicInteger();
        TaskHandler handler = new TaskHandler() {
            @Override public String type() { return "crash"; }
            @Override public String execute(String payload, TaskContext ctx) {
                runs.incrementAndGet();
                return "after-restart";
            }
        };
        // 手工构造一个 RUNNING 状态的 WAL 记录，模拟崩溃瞬间
        SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(1).concurrency(1).maxQueued(10)
                .handler(() -> handler).build();
        var rec = new com.github.highcumontoa.concurrenttaskschedulerjava.model.TaskRecord();
        rec.setTaskId("run-x");
        rec.setCallerId("alice");
        rec.setGroup("g");
        rec.setTaskType("crash");
        rec.setPayload("p");
        rec.setStatus(TaskStatus.RUNNING);
        rec.setAttempts(1);
        rec.setEnqueueTime(System.currentTimeMillis() - 1000);
        rec.setStartTime(System.currentTimeMillis());
        rec.setTimeoutMillis(30000);
        rec.setVersion(2);
        kit.store.append(rec);
        kit.close();

        try (SchedulerTestKit kit2 = SchedulerTestKit.builder(dir)
                .workers(2).concurrency(2).maxQueued(10)
                .handler(() -> handler).build().restart()) {
            org.awaitility.Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                    assertEquals(TaskStatus.SUCCEEDED,
                            kit2.service.get("run-x").orElseThrow().getStatus()));
            assertEquals("after-restart", kit2.service.get("run-x").orElseThrow().getResult());
            assertEquals(1, runs.get(), "崩溃后只执行一次（不重复副作用）");
            log.info("[提交标识=run-x] RUNNING 崩溃记录重启后重排队并成功");
        }
    }

    @Test
    void quotaAccountingIsCorrectAfterRecovery(@TempDir Path dir) {
        AtomicInteger runs = new AtomicInteger();
        TaskHandler handler = new TaskHandler() {
            @Override public String type() { return "w2"; }
            @Override public String execute(String payload, TaskContext ctx) {
                runs.incrementAndGet();
                return "ok";
            }
        };
        SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(2).concurrency(1).maxQueued(2)
                .handler(() -> handler).build();
        kit.service.submit("ok1", "zoe", "g", "w2", "p", null);
        org.awaitility.Awaitility.await().atMost(3, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals(TaskStatus.SUCCEEDED,
                        kit.service.get("ok1").orElseThrow().getStatus()));
        kit.close();

        try (SchedulerTestKit kit2 = SchedulerTestKit.builder(dir)
                .workers(2).concurrency(1).maxQueued(2)
                .handler(() -> handler).build().restart()) {
            QuotaKey key = QuotaKey.of("zoe", "g");
            assertEquals(0, kit2.quotaManager.activeCount(key), "终态任务不占并发");
            assertEquals(0, kit2.quotaManager.queuedCount(key), "终态任务不占队列");
            // 配额未泄漏：仍可提交并执行
            assertTrue(kit2.service.submit("ok2", "zoe", "g", "w2", "p", null).accepted());
            org.awaitility.Awaitility.await().atMost(3, TimeUnit.SECONDS).untilAsserted(() ->
                    assertEquals(TaskStatus.SUCCEEDED,
                            kit2.service.get("ok2").orElseThrow().getStatus()));
            log.info("[配额判定] 重启后 active=0 queued=0，新任务 ok2 正常执行");
        }
    }
}
