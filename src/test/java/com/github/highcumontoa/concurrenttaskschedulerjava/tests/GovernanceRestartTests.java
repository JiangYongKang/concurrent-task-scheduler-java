package com.github.highcumontoa.concurrenttaskschedulerjava.tests;

import com.github.highcumontoa.concurrenttaskschedulerjava.governance.GovernanceStatus;
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
 * 运行期治理状态的重启恢复测试：
 * 暂停状态与运行期配额调整都必须跨重启保持，重启后不得自动恢复放量；
 * 排队任务在暂停期间重启后仍排队，恢复后继续执行直至收敛。
 */
class GovernanceRestartTests {

    private static final Logger log = LoggerFactory.getLogger(GovernanceRestartTests.class);

    @Test
    void pauseSurvivesRestartAndDoesNotAutoResume(@TempDir Path dir) throws Exception {
        TestHandlers.LatchHandler latch = new TestHandlers.LatchHandler("block", 1);
        SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(4).concurrency(2).ratePerSecond(0).maxQueued(100)
                .handler(() -> latch).build();

        kit.service.pauseDispatch("alice", "g", "freeze-before-restart");
        assertTrue(kit.service.submit("r1", "alice", "g", "block", "p", null).accepted());
        Thread.sleep(150);
        assertEquals(0, latch.totalRuns.get(), "暂停期间任务不派发");
        assertEquals(TaskStatus.QUEUED, kit.service.get("r1").orElseThrow().getStatus());
        log.info("[暂停状态] 重启前 alice:g 已暂停，r1 已受理排队");
        kit.close();

        // 模拟进程重启：治理日志回放，暂停状态必须仍在
        try (SchedulerTestKit kit2 = kit.restart()) {
            GovernanceStatus st = kit2.service.governanceStatus("alice", "g");
            assertTrue(st.paused(), "暂停状态必须跨重启保持，不得自动恢复放量");
            assertTrue(st.lastChangeDescription().contains("freeze-before-restart"),
                    "最近操作信息应随重启恢复");
            log.info("[暂停状态] 重启后仍 paused=true，最近操作={}", st.lastChangeDescription());

            // 重启后暂停期间新提交同样受理排队
            assertTrue(kit2.service.submit("r2", "alice", "g", "block", "p", null).accepted());
            Thread.sleep(200);
            assertEquals(0, latch.totalRuns.get(), "重启后仍不得派发");
            assertEquals(TaskStatus.QUEUED, kit2.service.get("r1").orElseThrow().getStatus());
            assertEquals(TaskStatus.QUEUED, kit2.service.get("r2").orElseThrow().getStatus());
            assertEquals(2, kit2.service.governanceStatus("alice", "g").queued());
            log.info("[配额判定] 重启后暂停中：r1,r2 均排队，0 启动，queued=2");

            // 显式恢复后才放量，两个任务都成功，终态收敛
            kit2.service.resumeDispatch("alice", "g", "manual-resume");
            latch.release.countDown();
            org.awaitility.Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                assertEquals(TaskStatus.SUCCEEDED, kit2.service.get("r1").orElseThrow().getStatus());
                assertEquals(TaskStatus.SUCCEEDED, kit2.service.get("r2").orElseThrow().getStatus());
            });
            assertFalse(kit2.service.governanceStatus("alice", "g").paused());
            assertEquals(2, latch.totalRuns.get());
            log.info("[状态变化] 显式恢复后 r1,r2 全部 SUCCEEDED，暂停解除");
        }
    }

    @Test
    void runtimeQuotaAdjustmentSurvivesRestart(@TempDir Path dir) throws Exception {
        TestHandlers.LatchHandler latch = new TestHandlers.LatchHandler("block", 1);
        SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(8).concurrency(4).ratePerSecond(0).maxQueued(100)
                .handler(() -> latch).build();
        // 运行期将并发调小到 1、每秒启动调为 1、排队上限调为 3
        kit.service.adjustQuota("bob", "g", 1, 1, 3, "shrink-runtime");
        log.info("[配额判定] 重启前运行期调整为 maxConcurrency=1 rate=1 maxQueued=3");
        kit.close();

        try (SchedulerTestKit kit2 = kit.restart()) {
            GovernanceStatus st = kit2.service.governanceStatus("bob", "g");
            assertEquals(1, st.limits().maxConcurrency(), "运行期并发上限必须跨重启保持");
            assertEquals(1, st.limits().rateLimitPerSecond(), "运行期启动速率必须跨重启保持");
            assertEquals(3, st.limits().maxQueued(), "运行期排队上限必须跨重启保持");
            assertTrue(st.lastChangeDescription().contains("adjust-limits"));
            log.info("[配额判定] 重启后生效配额仍为 1/1/3，最近操作={}",
                    st.lastChangeDescription());

            // 排队上限 3：1 个执行（先占住）+ 3 个排队，第 5 个必须拒绝
            assertTrue(kit2.service.submit("t1", "bob", "g", "block", "p", null).accepted());
            assertTrue(latch.started.await(2, TimeUnit.SECONDS));
            assertTrue(kit2.service.submit("t2", "bob", "g", "block", "p", null).accepted());
            assertTrue(kit2.service.submit("t3", "bob", "g", "block", "p", null).accepted());
            assertTrue(kit2.service.submit("t4", "bob", "g", "block", "p", null).accepted());
            var rejected = kit2.service.submit("t5", "bob", "g", "block", "p", null);
            assertFalse(rejected.accepted());
            assertEquals(com.github.highcumontoa.concurrenttaskschedulerjava.model.RejectReason.QUOTA_EXHAUSTED,
                    rejected.rejectReason());
            log.info("[配额判定] 重启后排队上限 3 生效：t5 被拒绝 QUOTA_EXHAUSTED");

            latch.release.countDown();
            org.awaitility.Awaitility.await().atMost(6, TimeUnit.SECONDS).untilAsserted(() -> {
                for (String id : new String[]{"t1", "t2", "t3", "t4"}) {
                    assertEquals(TaskStatus.SUCCEEDED,
                            kit2.service.get(id).orElseThrow().getStatus());
                }
            });
            log.info("[状态变化] 已受理的 t1~t4 全部 SUCCEEDED，终态收敛");
        }
    }
}
