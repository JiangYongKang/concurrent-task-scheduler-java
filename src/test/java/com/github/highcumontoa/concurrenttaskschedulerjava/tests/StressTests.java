package com.github.highcumontoa.concurrenttaskschedulerjava.tests;

import com.github.highcumontoa.concurrenttaskschedulerjava.handler.TaskContext;
import com.github.highcumontoa.concurrenttaskschedulerjava.handler.TaskHandler;
import com.github.highcumontoa.concurrenttaskschedulerjava.model.TaskStatus;
import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaKey;
import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaManager;
import com.github.highcumontoa.concurrenttaskschedulerjava.service.SubmitResult;
import com.github.highcumontoa.concurrenttaskschedulerjava.tests.support.SchedulerTestKit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** 高并发压力：多调用方同时提交，分组并发与全局线程上限都不得突破。 */
class StressTests {

    private static final Logger log = LoggerFactory.getLogger(StressTests.class);

    @Test
    void concurrentSubmissionsNeverBreakLimits(@TempDir Path dir) throws Exception {
        int callers = 6;
        int perCaller = 20;
        int perGroupConcurrency = 2;
        int workers = 4;

        Map<String, AtomicInteger> current = new ConcurrentHashMap<>();
        Map<String, AtomicInteger> maxSeen = new ConcurrentHashMap<>();
        AtomicInteger globalCurrent = new AtomicInteger();
        AtomicInteger globalMax = new AtomicInteger();
        AtomicInteger totalRuns = new AtomicInteger();
        CountDownLatch completed = new CountDownLatch(callers * perCaller);

        TaskHandler handler = new TaskHandler() {
            @Override public String type() { return "stress"; }
            @Override public String execute(String payload, TaskContext ctx) throws Exception {
                String key = ctx.callerId() + ":" + ctx.group();
                AtomicInteger cur = current.computeIfAbsent(key, k -> new AtomicInteger());
                AtomicInteger max = maxSeen.computeIfAbsent(key, k -> new AtomicInteger());
                int n = cur.incrementAndGet();
                max.accumulateAndGet(n, Math::max);
                int g = globalCurrent.incrementAndGet();
                globalMax.accumulateAndGet(g, Math::max);
                totalRuns.incrementAndGet();
                try {
                    Thread.sleep(5);
                } finally {
                    cur.decrementAndGet();
                    globalCurrent.decrementAndGet();
                    completed.countDown();
                }
                return "ok";
            }
        };

        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(workers)
                .concurrency(perGroupConcurrency)
                .ratePerSecond(0)
                .maxQueued(perCaller)
                .timeout(30_000)
                .handler(() -> handler).build()) {

            ExecutorService pool = Executors.newFixedThreadPool(callers);
            CountDownLatch start = new CountDownLatch(1);
            for (int c = 0; c < callers; c++) {
                final String caller = "user-" + c;
                pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < perCaller; i++) {
                        SubmitResult r = kit.service.submit(caller + "-" + i, caller,
                                "grp", "stress", "p", null);
                        assertTrue(r.accepted() || r.duplicate(),
                                "排队容量充足时提交必须被接受");
                    }
                    return null;
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

            assertTrue(completed.await(30, TimeUnit.SECONDS), "所有任务应执行完成");
            for (int c = 0; c < callers; c++) {
                AtomicInteger max = maxSeen.get("user-" + c + ":grp");
                assertNotNull(max);
                int seen = max.get();
                assertTrue(seen <= perGroupConcurrency,
                        "caller 维度并发不得超过 " + perGroupConcurrency + "，观测 " + seen);
            }
            assertTrue(globalMax.get() <= workers,
                    "全局并发不得超过 worker 数 " + workers + "，观测 " + globalMax.get());
            assertEquals(callers * perCaller, totalRuns.get(), "每个任务恰好执行一次");

            for (int c = 0; c < callers; c++) {
                for (int i = 0; i < perCaller; i++) {
                    String id = "user-" + c + "-" + i;
                    assertEquals(TaskStatus.SUCCEEDED,
                            kit.service.get(id).orElseThrow().getStatus());
                }
            }
            QuotaKey key = QuotaKey.of("user-0", "grp");
            assertEquals(0, kit.quotaManager.activeCount(key), "终态后并发全部释放");
            assertEquals(0, kit.quotaManager.queuedCount(key), "终态后排队全部释放");
            log.info("[压测] 任务={} 每caller峰值并发≤{} 全局峰值并发={}（上限 {}/组, {} 全局）",
                    totalRuns.get(), perGroupConcurrency, globalMax.get(),
                    perGroupConcurrency, workers);
        }
    }
}
