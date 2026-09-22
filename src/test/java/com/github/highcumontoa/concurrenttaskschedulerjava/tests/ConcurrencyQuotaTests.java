package com.github.highcumontoa.concurrenttaskschedulerjava.tests;

import com.github.highcumontoa.concurrenttaskschedulerjava.model.RejectReason;
import com.github.highcumontoa.concurrenttaskschedulerjava.model.TaskStatus;
import com.github.highcumontoa.concurrenttaskschedulerjava.service.SubmitResult;
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
 * 并发提交与配额上限测试：
 * 同一 caller:group 最多按 maxConcurrency 并发，超出的稳定排队；
 * 排队上限触发后拒绝且原因为 QUOTA_EXHAUSTED；不同组互不影响。
 */
class ConcurrencyQuotaTests {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyQuotaTests.class);

    @Test
    void concurrencyNeverExceedsQuotaAndExcessQueues(@TempDir Path dir) throws Exception {
        TestHandlers.LatchHandler latch = new TestHandlers.LatchHandler("block", 2);
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(8).concurrency(2).ratePerSecond(0).maxQueued(100)
                .handler(() -> latch)
                .build()) {

            // 并发提交 5 个任务
            for (int i = 1; i <= 5; i++) {
                SubmitResult r = kit.service.submit("c" + i, "alice", "g1", "block",
                        "p", null);
                assertTrue(r.accepted(), "task c" + i + " should be accepted");
                log.info("[提交标识=c{}] 已提交, accepted={}", i, r.accepted());
            }

            // 恰好 2 个开始执行，其余保持 QUEUED
            assertTrue(latch.started.await(2, TimeUnit.SECONDS), "两个配额槽位应被占用");
            // 给派发器一点时间后确认不会有第 3 个启动
            Thread.sleep(150);
            assertEquals(2, latch.totalRuns.get(), "并发不得超过配额 2");
            assertEquals(2, latch.maxSeen.get(), "观测到的最大并发必须为 2");
            assertEquals(TaskStatus.QUEUED, kit.service.get("c3").orElseThrow().getStatus());
            assertEquals(2, kit.quotaManager.activeCount(
                    com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaKey.of("alice", "g1")));
            log.info("[配额判定] active={} 上限=2，c3~c5 稳定排队", latch.current.get());

            // 放行后，其余任务依次执行，全部成功，执行总数=5
            latch.release.countDown();
            org.awaitility.Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                for (int i = 1; i <= 5; i++) {
                    assertEquals(TaskStatus.SUCCEEDED,
                            kit.service.get("c" + i).orElseThrow().getStatus(),
                            "c" + i + " should succeed");
                }
            });
            assertEquals(5, latch.totalRuns.get(), "每个任务恰好执行一次");
            log.info("[状态变化] 5 个任务全部 SUCCEEDED，总执行次数={}", latch.totalRuns.get());
        }
    }

    @Test
    void queueOverflowIsRejectedWithStableReason(@TempDir Path dir) throws Exception {
        TestHandlers.LatchHandler latch = new TestHandlers.LatchHandler("block", 1);
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(4).concurrency(1).ratePerSecond(0).maxQueued(2)
                .handler(() -> latch)
                .build()) {

            // 1 个执行 + 2 个排队 = 容量 3
            assertTrue(kit.service.submit("q1", "bob", "g", "block", "p", null).accepted());
            assertTrue(latch.started.await(2, TimeUnit.SECONDS));
            assertTrue(kit.service.submit("q2", "bob", "g", "block", "p", null).accepted());
            assertTrue(kit.service.submit("q3", "bob", "g", "block", "p", null).accepted());

            SubmitResult rejected = kit.service.submit("q4", "bob", "g", "block", "p", null);
            assertFalse(rejected.accepted());
            assertFalse(rejected.duplicate());
            assertEquals(RejectReason.QUOTA_EXHAUSTED, rejected.rejectReason());
            log.info("[提交标识=q4] 拒绝原因={} 信息={}", rejected.rejectReason(),
                    rejected.message());

            latch.release.countDown();
            org.awaitility.Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                assertEquals(TaskStatus.SUCCEEDED, kit.service.get("q1").orElseThrow().getStatus());
                assertEquals(TaskStatus.SUCCEEDED, kit.service.get("q2").orElseThrow().getStatus());
                assertEquals(TaskStatus.SUCCEEDED, kit.service.get("q3").orElseThrow().getStatus());
            });
            // 被拒绝的任务绝不执行、绝不占配额
            assertTrue(kit.service.get("q4").isEmpty());
            assertEquals(3, latch.totalRuns.get());
        }
    }

    @Test
    void differentGroupsAreIsolated(@TempDir Path dir) throws Exception {
        TestHandlers.LatchHandler latch = new TestHandlers.LatchHandler("block", 4);
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(8).concurrency(2).ratePerSecond(0).maxQueued(100)
                .handler(() -> latch)
                .build()) {
            // 两个组各占满 2，合计 4 并发
            for (String id : new String[]{"a1", "a2", "b1", "b2"}) {
                String group = id.startsWith("a") ? "ga" : "gb";
                assertTrue(kit.service.submit(id, "carol", group, "block", "p", null).accepted());
            }
            assertTrue(latch.started.await(2, TimeUnit.SECONDS));
            Thread.sleep(100);
            assertEquals(4, latch.totalRuns.get(), "不同组配额相互独立");
            latch.release.countDown();
        }
    }

    @Test
    void rateLimitsStartsPerSecond(@TempDir Path dir) throws Exception {
        // 并发额度充足但每秒最多启动 1 个：第二个任务必须等到下一自然秒
        java.util.concurrent.atomic.AtomicInteger runs = new java.util.concurrent.atomic.AtomicInteger();
        java.util.List<Long> startTimes = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        com.github.highcumontoa.concurrenttaskschedulerjava.handler.TaskHandler h =
                new com.github.highcumontoa.concurrenttaskschedulerjava.handler.TaskHandler() {
                    @Override public String type() { return "instant"; }
                    @Override public String execute(String payload,
                            com.github.highcumontoa.concurrenttaskschedulerjava.handler.TaskContext ctx) {
                        startTimes.add(System.currentTimeMillis());
                        runs.incrementAndGet();
                        return "ok";
                    }
                };
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(4).concurrency(4).ratePerSecond(1).maxQueued(100)
                .handler(() -> h)
                .build()) {
            kit.service.submit("r1", "dave", "g", "instant", "p", null);
            kit.service.submit("r2", "dave", "g", "instant", "p", null);
            org.awaitility.Awaitility.await().atMost(5, TimeUnit.SECONDS).until(
                    () -> runs.get() >= 2);
            long secondA = startTimes.get(0) / 1000;
            long secondB = startTimes.get(1) / 1000;
            assertTrue(secondB > secondA,
                    "速率限制 1/s：两次启动必须落在不同自然秒窗口, ts=" + startTimes);
            log.info("[配额判定] 速率限制 1/s，启动时间戳 {} (秒窗口 {} 与 {})",
                    startTimes, secondA, secondB);
        }
    }

}
