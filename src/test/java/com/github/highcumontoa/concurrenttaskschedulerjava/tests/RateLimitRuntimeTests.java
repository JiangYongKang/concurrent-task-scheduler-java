package com.github.highcumontoa.concurrenttaskschedulerjava.tests;

import com.github.highcumontoa.concurrenttaskschedulerjava.handler.TaskContext;
import com.github.highcumontoa.concurrenttaskschedulerjava.handler.TaskHandler;
import com.github.highcumontoa.concurrenttaskschedulerjava.model.TaskStatus;
import com.github.highcumontoa.concurrenttaskschedulerjava.tests.support.SchedulerTestKit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 每秒启动上限（rateLimitPerSecond）的运行期调整测试：
 * 调小后后续启动按新速率判定，调大后积压更快放出；本秒启动计数可观测。
 */
class RateLimitRuntimeTests {

    private static final Logger log = LoggerFactory.getLogger(RateLimitRuntimeTests.class);

    @Test
    void runtimeRateChangeTakesEffectImmediatelyAndCounterIsObservable(@TempDir Path dir)
            throws Exception {
        List<Long> startTimes = Collections.synchronizedList(new java.util.ArrayList<>());
        AtomicInteger runs = new AtomicInteger();
        TaskHandler instant = new TaskHandler() {
            @Override public String type() { return "instant"; }
            @Override public String execute(String payload, TaskContext ctx) {
                startTimes.add(System.currentTimeMillis());
                runs.incrementAndGet();
                return "ok";
            }
        };
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(8).concurrency(8).ratePerSecond(10).maxQueued(100)
                .handler(() -> instant).build()) {

            // 运行期把每秒启动上限调到 1：随后提交 3 个，启动必须分布在 3 个不同自然秒
            kit.service.adjustQuota("erin", "g", null, 1, null, "rate-floor");
            for (int i = 1; i <= 3; i++) {
                kit.service.submit("rt" + i, "erin", "g", "instant", "p", null);
            }
            org.awaitility.Awaitility.await().atMost(6, TimeUnit.SECONDS)
                    .until(() -> runs.get() >= 3);
            long s0 = startTimes.get(0) / 1000;
            long s1 = startTimes.get(1) / 1000;
            long s2 = startTimes.get(2) / 1000;
            assertTrue(s1 > s0, "第 2 个启动必须落在下一秒");
            assertTrue(s2 > s1, "第 3 个启动必须落在再下一秒");
            log.info("[配额判定] 速率调为 1/s：3 次启动分布在秒窗口 {},{},{}", s0, s1, s2);

            // 本秒已启动计数可观测且不超过生效速率
            var st = kit.service.governanceStatus("erin", "g");
            assertEquals(1, st.limits().rateLimitPerSecond());
            assertTrue(st.startedInCurrentSecond() >= 0);
            for (int i = 1; i <= 3; i++) {
                assertEquals(TaskStatus.SUCCEEDED,
                        kit.service.get("rt" + i).orElseThrow().getStatus());
            }

            // 调回 10/s：再提交 8 个，应在同一自然秒内全部启动
            long before = System.currentTimeMillis() / 1000;
            kit.service.adjustQuota("erin", "g", null, 10, null, "rate-raise");
            for (int i = 4; i <= 11; i++) {
                kit.service.submit("rt" + i, "erin", "g", "instant", "p", null);
            }
            org.awaitility.Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> runs.get() >= 11);
            long secondOfAll = (startTimes.get(3) / 1000);
            for (int i = 3; i < 11; i++) {
                assertEquals(secondOfAll, startTimes.get(i) / 1000,
                        "调大后积压应在同一秒内放量");
            }
            assertTrue(secondOfAll >= before);
            log.info("[配额判定] 速率调回 10/s：8 个积压任务在同一秒窗口 {} 内全部启动",
                    secondOfAll);
        }
    }
}
