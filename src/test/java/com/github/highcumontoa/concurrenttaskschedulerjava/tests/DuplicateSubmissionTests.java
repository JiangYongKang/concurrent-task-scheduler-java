package com.github.highcumontoa.concurrenttaskschedulerjava.tests;

import com.github.highcumontoa.concurrenttaskschedulerjava.handler.TaskContext;
import com.github.highcumontoa.concurrenttaskschedulerjava.handler.TaskHandler;
import com.github.highcumontoa.concurrenttaskschedulerjava.model.TaskStatus;
import com.github.highcumontoa.concurrenttaskschedulerjava.service.SubmitResult;
import com.github.highcumontoa.concurrenttaskschedulerjava.tests.support.SchedulerTestKit;
import com.github.highcumontoa.concurrenttaskschedulerjava.tests.support.TestHandlers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 重复提交幂等性测试：
 * 同一 taskId 无论串行还是高并发重复提交，只接受一次、只执行一次、只占一份配额；
 * 重复响应 duplicate=true 且返回当前状态。
 */
class DuplicateSubmissionTests {

    private static final Logger log = LoggerFactory.getLogger(DuplicateSubmissionTests.class);

    @Test
    void serialDuplicateReturnsExistingAndRunsOnce(@TempDir Path dir) throws Exception {
        TestHandlers.LatchHandler latch = new TestHandlers.LatchHandler("block", 1);
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(4).concurrency(2).maxQueued(100)
                .handler(() -> latch).build()) {

            SubmitResult first = kit.service.submit("dup-1", "alice", "g", "block", "p", null);
            assertTrue(first.accepted());
            assertTrue(latch.started.await(2, TimeUnit.SECONDS));

            SubmitResult second = kit.service.submit("dup-1", "alice", "g", "block", "p", null);
            assertTrue(second.duplicate());
            assertEquals(TaskStatus.RUNNING, second.status());
            log.info("[提交标识=dup-1] 第二次提交 duplicate=true 当前状态={}", second.status());

            latch.release.countDown();
            org.awaitility.Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                    assertEquals(TaskStatus.SUCCEEDED,
                            kit.service.get("dup-1").orElseThrow().getStatus()));

            // 终态后重复提交仍幂等，不再执行
            SubmitResult third = kit.service.submit("dup-1", "alice", "g", "block", "p", null);
            assertTrue(third.duplicate());
            assertEquals(TaskStatus.SUCCEEDED, third.status());
            assertEquals(1, latch.totalRuns.get(), "重复提交不得导致重复执行");
            log.info("[状态变化] dup-1 SUCCEEDED 后重复提交仍不执行, 总执行={}",
                    latch.totalRuns.get());
        }
    }

    @Test
    void concurrentDuplicatesAcceptedExactlyOnce(@TempDir Path dir) throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        AtomicInteger runs = new AtomicInteger();
        TaskHandler handler = new TaskHandler() {
            @Override public String type() { return "race"; }
            @Override public String execute(String payload, TaskContext ctx) throws Exception {
                entered.countDown();
                runs.incrementAndGet();
                Thread.sleep(100);
                return "ok";
            }
        };
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(8).concurrency(4).maxQueued(100)
                .handler(() -> handler).build()) {

            int threads = 20;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch go = new CountDownLatch(1);
            java.util.List<SubmitResult> results =
                    java.util.Collections.synchronizedList(new java.util.ArrayList<>());
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    go.await();
                    results.add(kit.service.submit("same-id", "alice", "g", "race", "p", null));
                    return null;
                });
            }
            go.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));

            long accepted = results.stream().filter(SubmitResult::accepted).count();
            long duplicates = results.stream().filter(SubmitResult::duplicate).count();
            assertEquals(1, accepted, "并发重复提交只能有一个被接受");
            assertEquals(threads - 1, duplicates, "其余全部判定为重复");
            log.info("[提交标识=same-id] 并发 {} 次: accepted={}, duplicate={}",
                    threads, accepted, duplicates);

            assertTrue(entered.await(2, TimeUnit.SECONDS));
            org.awaitility.Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                    assertEquals(TaskStatus.SUCCEEDED,
                            kit.service.get("same-id").orElseThrow().getStatus()));
            assertEquals(1, runs.get(), "高并发重复提交不得重复执行或重复占用配额");
            log.info("[状态变化] same-id 仅执行 {} 次 -> SUCCEEDED", runs.get());
        }
    }
}
