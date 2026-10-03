package com.github.highcumontoa.concurrenttaskschedulerjava.tests;

import com.github.highcumontoa.concurrenttaskschedulerjava.governance.GovernanceScope;
import com.github.highcumontoa.concurrenttaskschedulerjava.handler.TaskContext;
import com.github.highcumontoa.concurrenttaskschedulerjava.handler.TaskHandler;
import com.github.highcumontoa.concurrenttaskschedulerjava.model.TaskStatus;
import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaKey;
import com.github.highcumontoa.concurrenttaskschedulerjava.tests.support.SchedulerTestKit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 并发提交与并发治理同时进行的边界测试：
 * 无论调整如何穿插，实际并发永不超过“当前生效上限”（更不会突破曾经出现过的最大值）；
 * 最终所有已受理任务状态稳定收敛；运行态数字（active/queued）与真实调度自洽，
 * 不出现已拒绝/已结束任务残留在排队统计中的矛盾。
 */
class ConcurrentGovernanceTests {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentGovernanceTests.class);

    /** 短任务：记录观测到的最大/在跑并发。 */
    static final class BusyHandler implements TaskHandler {
        final AtomicInteger current = new AtomicInteger();
        final AtomicInteger maxSeen = new AtomicInteger();
        final AtomicInteger runs = new AtomicInteger();
        final long busyMillis;

        BusyHandler(long busyMillis) {
            this.busyMillis = busyMillis;
        }

        @Override public String type() { return "busy"; }

        @Override
        public String execute(String payload, TaskContext ctx) throws Exception {
            int n = current.incrementAndGet();
            maxSeen.accumulateAndGet(n, Math::max);
            runs.incrementAndGet();
            try {
                long deadline = System.currentTimeMillis() + busyMillis;
                while (System.currentTimeMillis() < deadline) {
                    if (ctx.cancelled()) {
                        throw new InterruptedException("cancelled");
                    }
                    Thread.sleep(5);
                }
            } finally {
                current.decrementAndGet();
            }
            return "ok";
        }
    }

    @Test
    void concurrentSubmissionsAndAdjustmentsNeverBreakLimitsAndConverge(@TempDir Path dir)
            throws Exception {
        BusyHandler handler = new BusyHandler(20);
        final int submissions = 120;
        final int maxConcurrency = 6;
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(8).concurrency(maxConcurrency).ratePerSecond(0).maxQueued(0)
                .handler(() -> handler).build()) {
            QuotaKey key = QuotaKey.of("alice", "g");

            ExecutorService pool = Executors.newFixedThreadPool(10);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();

            // 8 个提交线程并发提交 120 个任务（maxQueued=0 不限）
            for (int t = 0; t < 8; t++) {
                final int base = t;
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int i = base; i < submissions; i += 8) {
                        kit.service.submit("c-" + i, "alice", "g", "busy", "p", null);
                    }
                    return null;
                }));
            }
            // 2 个调整线程：在 1..6 之间随机并发地改并发上限（全为合法值）
            for (int t = 0; t < 2; t++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < 40; i++) {
                        int v = 1 + (int) (Math.random() * maxConcurrency);
                        kit.service.adjustQuota("alice", "g", v, null, null, "chaos-" + v);
                        Thread.sleep(5);
                    }
                    return null;
                }));
            }
            // 1 个状态查询线程：持续读取运行态并做自洽性校验
            AtomicInteger observed = new AtomicInteger();
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < 100; i++) {
                    var st = kit.service.governanceStatus("alice", "g");
                    observed.incrementAndGet();
                    assertTrue(st.active() >= 0 && st.queued() >= 0, "计数不得为负");
                    // active 不得超过全局 worker 数，也不得超过曾出现过的最大配置 6
                    assertTrue(st.active() <= maxConcurrency,
                            "active=" + st.active() + " 超过历史最大上限 " + maxConcurrency);
                    // active + queued 不得超过“已受理但未到终态”的任务总数
                    assertTrue(st.active() + st.queued() <= submissions,
                            "active+queued 超过已受理总数: " + st);
                    Thread.sleep(3);
                }
                return null;
            }));

            start.countDown();
            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
            pool.shutdown();

            // 调整线程结束后把上限恢复到最大，保证全部任务能尽快收敛
            kit.service.adjustQuota("alice", "g", maxConcurrency, null, null, "restore");
            org.awaitility.Awaitility.await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
                var st = kit.service.governanceStatus("alice", "g");
                assertEquals(0, st.active() + st.queued(),
                        "最终 active/queued 必须归零，实际=" + st);
            });
            int succeeded = 0;
            for (int i = 0; i < submissions; i++) {
                var rec = kit.service.get("c-" + i).orElseThrow();
                assertEquals(TaskStatus.SUCCEEDED, rec.getStatus(),
                        "已受理任务必须稳定收敛为 SUCCEEDED: c-" + i);
                succeeded++;
            }
            assertEquals(submissions, succeeded);
            assertEquals(submissions, handler.runs.get(), "每个已受理任务恰好执行一次");
            assertTrue(handler.maxSeen.get() <= maxConcurrency,
                    "实际观测并发 " + handler.maxSeen.get() + " 不得突破历史最大上限 6");
            var finalStatus = kit.service.governanceStatus("alice", "g");
            assertEquals(0, finalStatus.active());
            assertEquals(0, finalStatus.queued(), "终态后不得有任务残留在排队统计");
            assertTrue(finalStatus.lastChangeTimeMillis() > 0);
            log.info("[配额判定] 并发混沌测试：提交={} 执行={} 观测最大并发={} 状态采样={}",
                    submissions, handler.runs.get(), handler.maxSeen.get(), observed.get());
            log.info("[状态变化] 全部 {} 个任务 SUCCEEDED，active=0 queued=0，最近操作={}",
                    submissions, finalStatus.lastChangeDescription());
        }
    }

    @Test
    void invalidConcurrentAdjustmentsNeverCorruptEffectiveLimits(@TempDir Path dir)
            throws Exception {
        BusyHandler handler = new BusyHandler(5);
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(4).concurrency(3).ratePerSecond(0).maxQueued(0)
                .handler(() -> handler).build()) {

            ExecutorService pool = Executors.newFixedThreadPool(6);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            // 合法与非法调整并发交错
            for (int i = 0; i < 3; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int j = 0; j < 50; j++) {
                        kit.service.adjustQuota("alice", "g", 2, null, null, "valid-2");
                        Thread.sleep(2);
                    }
                    return null;
                }));
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int j = 0; j < 50; j++) {
                        try {
                            kit.service.adjustQuota("alice", "g", -3, null, null, "invalid");
                            fail("负数调整必须被拒绝");
                        } catch (IllegalArgumentException expected) {
                            // 预期：拒绝且不生效
                        }
                        Thread.sleep(2);
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get(20, TimeUnit.SECONDS);
            }
            pool.shutdown();

            var st = kit.service.governanceStatus("alice", "g");
            assertEquals(2, st.limits().maxConcurrency(), "非法值绝不能污染生效配额");
            assertTrue(st.limits().maxConcurrency() >= 0);

            // 生效配额 2 下提交一批任务，实际并发绝不超过 2
            for (int i = 0; i < 20; i++) {
                kit.service.submit("v-" + i, "alice", "g", "busy", "p", null);
            }
            org.awaitility.Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() ->
                    assertEquals(0, kit.service.governanceStatus("alice", "g").active()
                            + kit.service.governanceStatus("alice", "g").queued()));
            for (int i = 0; i < 20; i++) {
                assertEquals(TaskStatus.SUCCEEDED,
                        kit.service.get("v-" + i).orElseThrow().getStatus());
            }
            assertTrue(handler.maxSeen.get() <= 2,
                    "生效上限 2，观测最大并发=" + handler.maxSeen.get());
            log.info("[配额判定] 合法/非法调整并发交错后生效上限稳定=2，观测最大并发={}",
                    handler.maxSeen.get());
        }
    }
}
