package com.github.highcumontoa.concurrenttaskschedulerjava.tests;

import com.github.highcumontoa.concurrenttaskschedulerjava.model.TaskStatus;
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
 * 重试与终止策略测试：
 * 可重试错误按指数退避重试，成功后 attempts 可解释；
 * 超过最大重试次数 -> FAILED，最终原因保留；
 * 不可重试错误 -> 立即 FAILED，不产生额外执行；
 * 退避期间取消 -> CANCELLED，不再重试。
 */
class RetryTests {

    private static final Logger log = LoggerFactory.getLogger(RetryTests.class);

    @Test
    void retryableErrorsRetryWithBackoffThenSucceed(@TempDir Path dir) {
        TestHandlers.FlakyHandler flaky = new TestHandlers.FlakyHandler("flaky", 2, false);
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(2).concurrency(2).maxQueued(10)
                .maxRetries(3).backoffBase(40)
                .handler(() -> flaky).build()) {

            long t0 = System.currentTimeMillis();
            kit.service.submit("f1", "alice", "g", "flaky", "p", null);
            org.awaitility.Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertEquals(TaskStatus.SUCCEEDED,
                            kit.service.get("f1").orElseThrow().getStatus()));
            long elapsed = System.currentTimeMillis() - t0;
            var rec = kit.service.get("f1").orElseThrow();
            assertEquals(3, rec.getAttempts(), "失败 2 次后第 3 次成功");
            assertEquals("ok-after-2", rec.getResult());
            // 退避: 40 + 80 = 至少 ~120ms
            assertTrue(elapsed >= 110, "两次退避应消耗时间, elapsed=" + elapsed);
            log.info("[提交标识=f1] attempts=3 退避后成功, 耗时={}ms, 原因已清空={}",
                    elapsed, rec.getErrorReason() == null);
        }
    }

    @Test
    void exhaustRetriesFailsWithExplainableReason(@TempDir Path dir) {
        TestHandlers.FlakyHandler alwaysFail = new TestHandlers.FlakyHandler("failer", 99, false);
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(2).concurrency(2).maxQueued(10)
                .maxRetries(2).backoffBase(20)
                .handler(() -> alwaysFail).build()) {

            kit.service.submit("f2", "alice", "g", "failer", "p", null);
            org.awaitility.Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertEquals(TaskStatus.FAILED,
                            kit.service.get("f2").orElseThrow().getStatus()));
            var rec = kit.service.get("f2").orElseThrow();
            assertEquals(3, rec.getAttempts(), "首次 + 2 次重试 = 3 次执行");
            assertTrue(rec.getErrorReason().contains("retryable error"),
                    "最终失败原因需可解释: " + rec.getErrorReason());
            assertEquals(IllegalStateException.class.getName(), rec.getErrorClass());
            assertEquals(3, alwaysFail.runs.get(), "不得超过最大重试次数执行");
            log.info("[提交标识=f2] 重试耗尽 -> FAILED attempts=3 原因={}", rec.getErrorReason());
        }
    }

    @Test
    void nonRetryableErrorFailsImmediately(@TempDir Path dir) {
        TestHandlers.FlakyHandler fatal = new TestHandlers.FlakyHandler("fatal", 1, true);
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(2).concurrency(2).maxQueued(10)
                .maxRetries(5).backoffBase(20)
                .handler(() -> fatal).build()) {

            kit.service.submit("f3", "alice", "g", "fatal", "p", null);
            org.awaitility.Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                    assertEquals(TaskStatus.FAILED,
                            kit.service.get("f3").orElseThrow().getStatus()));
            var rec = kit.service.get("f3").orElseThrow();
            assertEquals(1, rec.getAttempts(), "不可重试错误只能执行一次");
            assertTrue(rec.getErrorReason().contains("non-retryable error"),
                    "需标明不可重试: " + rec.getErrorReason());
            assertTrue(rec.getErrorReason().contains("boom-1"));
            log.info("[提交标识=f3] 不可重试 -> FAILED attempts=1 原因={}", rec.getErrorReason());
        }
    }

    @Test
    void cancelDuringBackoffStopsRetries(@TempDir Path dir) throws Exception {
        TestHandlers.FlakyHandler flaky = new TestHandlers.FlakyHandler("flaky2", 1, false);
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(2).concurrency(1).maxQueued(10)
                .maxRetries(5).backoffBase(5_000) // 长退避便于取消
                .handler(() -> flaky).build()) {

            kit.service.submit("f4", "alice", "g", "flaky2", "p", null);
            org.awaitility.Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                    assertEquals(TaskStatus.PENDING_RETRY,
                            kit.service.get("f4").orElseThrow().getStatus()));
            assertTrue(kit.service.cancel("f4", "give up"));
            assertEquals(TaskStatus.CANCELLED, kit.service.get("f4").orElseThrow().getStatus());
            log.info("[提交标识=f4] 退避中取消 -> CANCELLED");

            Thread.sleep(200);
            assertEquals(1, flaky.runs.get(), "取消后不得继续重试");
        }
    }
}
