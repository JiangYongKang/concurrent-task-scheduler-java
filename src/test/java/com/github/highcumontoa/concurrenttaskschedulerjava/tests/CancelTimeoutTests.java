package com.github.highcumontoa.concurrenttaskschedulerjava.tests;

import com.github.highcumontoa.concurrenttaskschedulerjava.model.TaskStatus;
import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaKey;
import com.github.highcumontoa.concurrenttaskschedulerjava.tests.support.SchedulerTestKit;
import com.github.highcumontoa.concurrenttaskschedulerjava.tests.support.TestHandlers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 取消与超时测试：
 * 排队中取消 -> CANCELLED 且释放排队配额，后续任务可顶入；
 * 执行中取消 -> 中断执行，CANCELLED 与 FAILED/TIMEOUT 可区分；
 * 执行超时 -> FAILED 且 errorReason 明确为 timeout，不重试；
 * 不响应中断的任务：超时后不重复执行，终态仍为 FAILED。
 */
class CancelTimeoutTests {

    private static final Logger log = LoggerFactory.getLogger(CancelTimeoutTests.class);

    @Test
    void cancelQueuedTaskReleasesQueueQuota(@TempDir Path dir) throws Exception {
        TestHandlers.LatchHandler latch = new TestHandlers.LatchHandler("block", 1);
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(4).concurrency(1).maxQueued(10)
                .handler(() -> latch).build()) {

            kit.service.submit("run1", "alice", "g", "block", "p", null);
            assertTrue(latch.started.await(2, TimeUnit.SECONDS));
            kit.service.submit("wait1", "alice", "g", "block", "p", null);
            kit.service.submit("wait2", "alice", "g", "block", "p", null);
            QuotaKey key = QuotaKey.of("alice", "g");
            assertEquals(2, kit.quotaManager.queuedCount(key));

            boolean cancelled = kit.service.cancel("wait1", "not needed");
            assertTrue(cancelled);
            assertEquals(TaskStatus.CANCELLED, kit.service.get("wait1").orElseThrow().getStatus());
            assertEquals(1, kit.quotaManager.queuedCount(key), "取消排队任务必须释放排队配额");
            log.info("[提交标识=wait1] 状态->CANCELLED, 排队剩余={}",
                    kit.quotaManager.queuedCount(key));

            // 再提交一个仍可入队（队列名额未泄漏）
            assertTrue(kit.service.submit("wait3", "alice", "g", "block", "p", null).accepted());

            latch.release.countDown();
            org.awaitility.Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                assertEquals(TaskStatus.SUCCEEDED, kit.service.get("run1").orElseThrow().getStatus());
                assertEquals(TaskStatus.SUCCEEDED, kit.service.get("wait2").orElseThrow().getStatus());
                assertEquals(TaskStatus.SUCCEEDED, kit.service.get("wait3").orElseThrow().getStatus());
            });
        }
    }

    @Test
    void cancelRunningTaskInterruptsAndReleasesConcurrency(@TempDir Path dir) throws Exception {
        TestHandlers.LatchHandler latch = new TestHandlers.LatchHandler("block", 1);
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(4).concurrency(1).maxQueued(10)
                .handler(() -> latch).build()) {

            kit.service.submit("long1", "alice", "g", "block", "p", null);
            assertTrue(latch.started.await(2, TimeUnit.SECONDS));
            assertEquals(1, kit.quotaManager.activeCount(QuotaKey.of("alice", "g")));

            assertTrue(kit.service.cancel("long1", "user abort"));
            org.awaitility.Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                    assertEquals(TaskStatus.CANCELLED,
                            kit.service.get("long1").orElseThrow().getStatus()));
            assertEquals(0, kit.quotaManager.activeCount(QuotaKey.of("alice", "g")),
                    "取消执行中任务必须释放并发槽位");
            String reason = kit.service.get("long1").orElseThrow().getErrorReason();
            assertTrue(reason.contains("user abort"), "取消原因需可解释: " + reason);
            log.info("[提交标识=long1] 状态->CANCELLED, 原因={}, active=0", reason);

            // 槽位释放后，新任务能立刻获取配额执行
            TestHandlers.LatchHandler latch2 = new TestHandlers.LatchHandler("block2", 1);
            kit.registry.register(latch2);
            kit.service.submit("next1", "alice", "g", "block2", "p", null);
            assertTrue(latch2.started.await(2, TimeUnit.SECONDS), "配额未泄漏，新任务应可执行");
            latch2.release.countDown();
        }
    }

    @Test
    void executionTimeoutFailsWithoutRetry(@TempDir Path dir) {
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(4).concurrency(2).maxQueued(10)
                .timeout(150).maxRetries(3)
                .sampleHandler().build()) {

            kit.service.submit("slow", "alice", "g", "sample", "sleep:2000", 150L);
            org.awaitility.Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                    assertEquals(TaskStatus.FAILED,
                            kit.service.get("slow").orElseThrow().getStatus()));
            var rec = kit.service.get("slow").orElseThrow();
            assertEquals(1, rec.getAttempts(), "超时不得重试");
            assertTrue(rec.getErrorReason().contains("execution timeout"),
                    "超时原因需明确: " + rec.getErrorReason());
            assertEquals(0, kit.quotaManager.activeCount(QuotaKey.of("alice", "g")),
                    "超时必须释放并发槽位");
            log.info("[提交标识=slow] 状态->FAILED(超时) attempts=1 原因={}", rec.getErrorReason());
        }
    }

    @Test
    void nonCooperativeTimedOutTaskIsNotRunTwice(@TempDir Path dir) {
        TestHandlers.IgnoringInterruptHandler ignoring =
                new TestHandlers.IgnoringInterruptHandler("stubborn", 800);
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(4).concurrency(2).maxQueued(10)
                .timeout(100).maxRetries(3)
                .handler(() -> ignoring).build()) {

            kit.service.submit("stub", "alice", "g", "stubborn", "p", 100L);
            // 超时后框架立即判 FAILED 并释放配额（与不协作的执行体隔离）
            org.awaitility.Awaitility.await().atMost(3, TimeUnit.SECONDS).untilAsserted(() ->
                    assertEquals(TaskStatus.FAILED,
                            kit.service.get("stub").orElseThrow().getStatus()));
            var rec = kit.service.get("stub").orElseThrow();
            assertEquals(1, rec.getAttempts());
            // 等待不协作线程真正结束
            org.awaitility.Awaitility.await().atMost(3, TimeUnit.SECONDS)
                    .until(() -> ignoring.runs.get() == 1);
            assertEquals(1, ignoring.runs.get(), "超时任务不得被重复执行");
            log.info("[提交标识=stub] 不协作任务超时后仅运行 {} 次", ignoring.runs.get());
        }
    }
}
