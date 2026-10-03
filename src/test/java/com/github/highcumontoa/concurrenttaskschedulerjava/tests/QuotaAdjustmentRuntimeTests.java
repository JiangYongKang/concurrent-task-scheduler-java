package com.github.highcumontoa.concurrenttaskschedulerjava.tests;

import com.github.highcumontoa.concurrenttaskschedulerjava.governance.GovernanceStatus;
import com.github.highcumontoa.concurrenttaskschedulerjava.handler.TaskContext;
import com.github.highcumontoa.concurrenttaskschedulerjava.handler.TaskHandler;
import com.github.highcumontoa.concurrenttaskschedulerjava.model.TaskStatus;
import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaKey;
import com.github.highcumontoa.concurrenttaskschedulerjava.tests.support.SchedulerTestKit;
import com.github.highcumontoa.concurrenttaskschedulerjava.tests.support.TestHandlers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 运行期配额调整测试：
 * 调小不中断在执行任务、状态不回退且后续按新上限放行；
 * 调大后积压任务按原 FIFO 顺序尽快放量；
 * 非法（负）值整次拒绝、原有效值保持不变。
 */
class QuotaAdjustmentRuntimeTests {

    private static final Logger log = LoggerFactory.getLogger(QuotaAdjustmentRuntimeTests.class);

    /**
     * 两段门闩处理器：payload="early" 的任务在 {@code earlyGate} 打开后结束，
     * 其余任务在 {@code allGate} 打开后结束。用于确定地只放一个在跑任务结束。
     */
    static final class TwoPhaseHandler implements TaskHandler {
        final CountDownLatch earlyGate = new CountDownLatch(1);
        final CountDownLatch allGate = new CountDownLatch(1);
        final AtomicInteger totalRuns = new AtomicInteger();
        final AtomicInteger maxSeen = new AtomicInteger();
        final AtomicInteger current = new AtomicInteger();

        @Override public String type() { return "twophase"; }

        @Override
        public String execute(String payload, TaskContext ctx) throws Exception {
            int n = current.incrementAndGet();
            maxSeen.accumulateAndGet(n, Math::max);
            totalRuns.incrementAndGet();
            try {
                CountDownLatch gate = "early".equals(payload) ? earlyGate : allGate;
                while (gate.getCount() > 0) {
                    if (ctx.cancelled()) {
                        throw new InterruptedException("cancelled");
                    }
                    gate.await(20, TimeUnit.MILLISECONDS);
                }
            } finally {
                current.decrementAndGet();
            }
            return "done";
        }
    }


    @Test
    void shrinkConcurrencyDoesNotInterruptRunningAndCapsFutureStarts(@TempDir Path dir) throws Exception {
        TwoPhaseHandler handler = new TwoPhaseHandler();
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(8).concurrency(2).ratePerSecond(0).maxQueued(100)
                .handler(() -> handler).build()) {
            QuotaKey key = QuotaKey.of("alice", "g1");

            // s1 标记为 early：稍后只放它一个结束；s2 继续占槽
            assertTrue(kit.service.submit("s1", "alice", "g1", "twophase", "early", null).accepted());
            assertTrue(kit.service.submit("s2", "alice", "g1", "twophase", "hold", null).accepted());
            assertTrue(kit.service.submit("s3", "alice", "g1", "twophase", "hold", null).accepted());
            assertTrue(kit.service.submit("s4", "alice", "g1", "twophase", "hold", null).accepted());
            org.awaitility.Awaitility.await().atMost(2, TimeUnit.SECONDS)
                    .until(() -> kit.service.governanceStatus("alice", "g1").active() == 2);
            assertEquals(2, handler.current.get());
            log.info("[配额判定] 初始 active=2/上限2 queued=2，s3,s4 排队");

            // 运行期把并发上限从 2 调小到 1
            var change = kit.service.adjustQuota("alice", "g1", 1, null, null, "traffic-spike");
            assertEquals(1, change.limits().maxConcurrency());
            GovernanceStatus st = kit.service.governanceStatus("alice", "g1");
            assertEquals(1, st.limits().maxConcurrency());
            assertEquals(2, st.active(), "调小不得中断在执行任务，active 仍为 2");
            assertEquals(TaskStatus.RUNNING, kit.service.get("s1").orElseThrow().getStatus());
            assertEquals(TaskStatus.RUNNING, kit.service.get("s2").orElseThrow().getStatus());
            log.info("[配额判定] 调小为 1：在跑 2 个不中断、状态不回退，后续按新上限判定");

            // 只让 s1 结束：active 降到 1，但新上限=1（s2 仍占着），不得放出 s3
            handler.earlyGate.countDown();
            org.awaitility.Awaitility.await().atMost(3, TimeUnit.SECONDS)
                    .until(() -> kit.service.governanceStatus("alice", "g1").active() == 1);
            Thread.sleep(200);
            assertEquals(2, handler.totalRuns.get(), "调小后不得按旧上限 2 放行第 3 个");
            assertEquals(2, kit.service.governanceStatus("alice", "g1").queued(),
                    "未放行的任务必须继续计入排队");
            assertEquals(TaskStatus.RUNNING, kit.service.get("s2").orElseThrow().getStatus(),
                    "在执行任务不得被中断");
            log.info("[配额判定] s1 结束后 active=1/上限1（s2 仍占槽），queued=2，无第 3 个启动");

            // 放开其余任务：上限 1 下串行执行，全部成功；资源上限始终未被突破
            handler.allGate.countDown();
            org.awaitility.Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                for (int i = 1; i <= 4; i++) {
                    assertEquals(TaskStatus.SUCCEEDED,
                            kit.service.get("s" + i).orElseThrow().getStatus());
                }
            });
            assertEquals(4, handler.totalRuns.get());
            assertEquals(2, handler.maxSeen.get(), "调小后观测到的最大并发不得超过旧上限");
            log.info("[状态变化] 调小后 4 个任务最终全部 SUCCEEDED，无中断无回退");
        }
    }

    @Test
    void growConcurrencyReleasesBacklogInFifoOrder(@TempDir Path dir) throws Exception {
        TestHandlers.LatchHandler latch = new TestHandlers.LatchHandler("block", 3);
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(8).concurrency(1).ratePerSecond(0).maxQueued(100)
                .handler(() -> latch).build()) {

            for (int i = 1; i <= 5; i++) {
                assertTrue(kit.service.submit("g" + i, "bob", "g", "block", "p", null).accepted());
            }
            org.awaitility.Awaitility.await().atMost(2, TimeUnit.SECONDS)
                    .until(() -> kit.service.governanceStatus("bob", "g").active() == 1);
            assertEquals(4, kit.service.governanceStatus("bob", "g").queued());
            log.info("[配额判定] 初始 active=1/上限1 queued=4，积压 g2..g5");

            // 运行期调大到 3：积压任务应按原顺序尽快放量到 3
            kit.service.adjustQuota("bob", "g", 3, null, null, "clear-backlog");
            assertTrue(latch.started.await(2, TimeUnit.SECONDS), "应再放出 2 个到上限 3");
            org.awaitility.Awaitility.await().atMost(2, TimeUnit.SECONDS)
                    .until(() -> kit.service.governanceStatus("bob", "g").active() == 3);
            assertEquals(3, latch.maxSeen.get(), "调大后并发应达到新上限 3");
            assertEquals(3, latch.totalRuns.get(), "先启动的必须是队头 g1,g2,g3");
            log.info("[配额判定] 调大为 3：按 FIFO 放量 g1,g2,g3，active=3 queued=2");

            latch.release.countDown();
            org.awaitility.Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                for (int i = 1; i <= 5; i++) {
                    assertEquals(TaskStatus.SUCCEEDED,
                            kit.service.get("g" + i).orElseThrow().getStatus());
                }
            });
            assertEquals(5, latch.totalRuns.get());
            log.info("[状态变化] 调大后 5 个任务全部 SUCCEEDED，FIFO 顺序保持");
        }
    }

    @Test
    void illegalValuesAreRejectedAndPreviousLimitsRemain(@TempDir Path dir) {
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(4).concurrency(2).ratePerSecond(0).maxQueued(100)
                .sampleHandler().build()) {
            QuotaKey key = QuotaKey.of("carol", "g");
            assertEquals(2, kit.quotaManager.limits(key).maxConcurrency());

            assertThrows(IllegalArgumentException.class,
                    () -> kit.service.adjustQuota("carol", "g", -1, null, null, "bad"));
            assertThrows(IllegalArgumentException.class,
                    () -> kit.service.adjustQuota("carol", "g", null, -5, null, "bad"));
            assertThrows(IllegalArgumentException.class,
                    () -> kit.service.adjustQuota("carol", "g", null, null, -9, "bad"));
            assertThrows(IllegalArgumentException.class,
                    () -> kit.service.adjustQuota("carol", "g", null, null, null, "noop"));
            assertThrows(IllegalArgumentException.class,
                    () -> kit.service.adjustQuota(null, null, 1, null, null, "no-scope"));
            log.info("[配额判定] 负数/全空/无作用域调整均被拒绝");

            // 原值必须完全不变
            GovernanceStatus st = kit.service.governanceStatus("carol", "g");
            assertEquals(2, st.limits().maxConcurrency(), "非法调整后原值保持");
            assertEquals(0, st.limits().rateLimitPerSecond());
            assertEquals(100, st.limits().maxQueued());
            assertEquals(-1, st.lastChangeTimeMillis(), "被拒绝的操作不得记录为最近操作");
            assertNull(st.lastChangeDescription());
            log.info("[配额判定] 拒绝后生效配额仍为 maxConcurrency=2 rate=0 queued=100");

            // 合法 PATCH 只改指定项，其余沿用原值
            var ok = kit.service.adjustQuota("carol", "g", null, 7, null, "rate-cap");
            assertEquals(2, ok.limits().maxConcurrency(), "PATCH 不应改动未指定项");
            assertEquals(7, ok.limits().rateLimitPerSecond());
            assertEquals(100, ok.limits().maxQueued());
            log.info("[配额判定] 合法 PATCH 仅 rateLimitPerSecond=7，其余项保持");
        }
    }
}
