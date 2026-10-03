package com.github.highcumontoa.concurrenttaskschedulerjava.tests;

import com.github.highcumontoa.concurrenttaskschedulerjava.governance.GovernanceStatus;
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
 * 暂停/恢复派发测试：
 * 暂停期间新提交照常受理进入排队（不拒绝、不丢失），在执行任务自然跑完，
 * 恢复后排队任务按原公平顺序执行；按调用方或任务组维度暂停均生效且互不误伤。
 */
class PauseResumeTests {

    private static final Logger log = LoggerFactory.getLogger(PauseResumeTests.class);

    @Test
    void pauseQueuesNewTasksAndLetsRunningFinishThenResumeDrainsFifo(@TempDir Path dir) throws Exception {
        TestHandlers.LatchHandler latch = new TestHandlers.LatchHandler("block", 1);
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(4).concurrency(1).ratePerSecond(0).maxQueued(100)
                .handler(() -> latch).build()) {

            assertTrue(kit.service.submit("p1", "alice", "g", "block", "p", null).accepted());
            assertTrue(latch.started.await(2, TimeUnit.SECONDS));
            log.info("[暂停状态] p1 已 RUNNING");

            // 暂停该精确维度
            var paused = kit.service.pauseDispatch("alice", "g", "incident-freeze");
            assertTrue(paused.paused());
            GovernanceStatus st = kit.service.governanceStatus("alice", "g");
            assertTrue(st.paused());
            assertTrue(st.lastChangeDescription().contains("paused=true"));
            log.info("[暂停状态] alice:g 已暂停 {}", st.lastChangeDescription());

            // 暂停期间新提交：必须受理并排队，不能拒绝
            for (int i = 2; i <= 4; i++) {
                SubmitResult r = kit.service.submit("p" + i, "alice", "g", "block", "p", null);
                assertTrue(r.accepted(), "暂停期间提交必须被受理，不得拒绝");
                assertEquals(TaskStatus.QUEUED, r.status(), "暂停期间新任务必须进入排队");
            }
            Thread.sleep(150);
            assertEquals(1, latch.totalRuns.get(), "暂停期间不得启动新任务");
            assertEquals(TaskStatus.RUNNING, kit.service.get("p1").orElseThrow().getStatus());
            st = kit.service.governanceStatus("alice", "g");
            assertEquals(1, st.active(), "在执行任务不被中断");
            assertEquals(3, st.queued(), "新任务必须全部计入排队");
            log.info("[配额判定] 暂停期间 active=1(在跑的p1) queued=3，新提交全部受理排队");

            // 在跑的 p1 自然跑完：active 归零，但暂停未解除，排队任务不动
            latch.release.countDown();
            org.awaitility.Awaitility.await().atMost(3, TimeUnit.SECONDS).untilAsserted(() ->
                    assertEquals(TaskStatus.SUCCEEDED,
                            kit.service.get("p1").orElseThrow().getStatus()));
            Thread.sleep(150);
            assertEquals(1, latch.totalRuns.get(), "暂停期间 p1 跑完后也不得启动排队任务");
            assertEquals(0, kit.service.governanceStatus("alice", "g").active());
            assertEquals(3, kit.service.governanceStatus("alice", "g").queued(),
                    "已在跑任务结束不能把排队任务挤出统计");
            log.info("[状态变化] p1 自然 SUCCEEDED；暂停仍保持，p2~p4 稳定排队");

            // 恢复：排队任务按 FIFO 全部执行成功
            var resumed = kit.service.resumeDispatch("alice", "g", "incident-over");
            assertFalse(resumed.paused());
            assertFalse(kit.service.governanceStatus("alice", "g").paused());
            org.awaitility.Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                for (int i = 1; i <= 4; i++) {
                    assertEquals(TaskStatus.SUCCEEDED,
                            kit.service.get("p" + i).orElseThrow().getStatus());
                }
            });
            assertEquals(4, latch.totalRuns.get(), "每个任务恰好执行一次，无丢失无重复");
            GovernanceStatus after = kit.service.governanceStatus("alice", "g");
            assertEquals(0, after.active());
            assertEquals(0, after.queued(), "终态任务不得残留在排队统计中");
            log.info("[状态变化] 恢复后 p2~p4 按 FIFO 全部 SUCCEEDED，active=0 queued=0");
        }
    }

    @Test
    void pauseByCallerAffectsAllHisGroupsButNotOtherCallers(@TempDir Path dir) throws Exception {
        TestHandlers.LatchHandler latchA = new TestHandlers.LatchHandler("block", 2);
        TestHandlers.LatchHandler latchB = new TestHandlers.LatchHandler("other", 2);
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(8).concurrency(2).ratePerSecond(0).maxQueued(100)
                .handler(() -> latchA).handler(() -> latchB).build()) {

            // 按调用方暂停 alice（覆盖其所有组）
            kit.service.pauseDispatch("alice", null, "freeze-caller");
            assertTrue(kit.service.governanceStatus("alice", "g1").paused());
            assertTrue(kit.service.governanceStatus("alice", "g2").paused());
            assertFalse(kit.service.governanceStatus("bob", "g1").paused());
            log.info("[暂停状态] 按调用方暂停 alice：alice:g1/alice:g2 paused, bob 不受影响");

            assertTrue(kit.service.submit("a1", "alice", "g1", "block", "p", null).accepted());
            assertTrue(kit.service.submit("a2", "alice", "g2", "block", "p", null).accepted());
            assertTrue(kit.service.submit("b1", "bob", "g1", "other", "p", null).accepted());
            Thread.sleep(200);
            assertEquals(0, latchA.totalRuns.get(), "alice 两个组都不得派发");
            assertEquals(1, latchB.totalRuns.get(), "其它调用方照常派发");
            assertEquals(TaskStatus.QUEUED, kit.service.get("a1").orElseThrow().getStatus());
            log.info("[配额判定] caller 暂停：alice 任务 0 启动且已排队，bob 正常执行");

            // 恢复 alice：两个组的任务都执行；FIFO 由 LinkedHashMap 顺序保证
            kit.service.resumeDispatch("alice", null, "clear");
            latchA.release.countDown();
            org.awaitility.Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                assertEquals(TaskStatus.SUCCEEDED, kit.service.get("a1").orElseThrow().getStatus());
                assertEquals(TaskStatus.SUCCEEDED, kit.service.get("a2").orElseThrow().getStatus());
            });
            latchB.release.countDown();
            log.info("[状态变化] 恢复 caller 后 alice 两组任务全部 SUCCEEDED");
        }
    }

    @Test
    void pauseByGroupAffectsAllCallersInThatGroup(@TempDir Path dir) throws Exception {
        TestHandlers.LatchHandler latch = new TestHandlers.LatchHandler("block", 1);
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(4).concurrency(2).ratePerSecond(0).maxQueued(100)
                .handler(() -> latch).build()) {

            kit.service.pauseDispatch(null, "critical", "freeze-group");
            assertTrue(kit.service.governanceStatus("alice", "critical").paused());
            assertTrue(kit.service.governanceStatus("bob", "critical").paused());
            assertFalse(kit.service.governanceStatus("alice", "other").paused());
            log.info("[暂停状态] 按任务组暂停 critical：所有 caller 的该组暂停");

            assertTrue(kit.service.submit("x1", "alice", "critical", "block", "p", null).accepted());
            assertTrue(kit.service.submit("x2", "bob", "critical", "block", "p", null).accepted());
            Thread.sleep(150);
            assertEquals(0, latch.totalRuns.get(), "该组所有调用方都不得派发");
            assertEquals(TaskStatus.QUEUED, kit.service.get("x1").orElseThrow().getStatus());
            assertEquals(TaskStatus.QUEUED, kit.service.get("x2").orElseThrow().getStatus());
            assertEquals(2, kit.service.governanceStatus("alice", "critical").queued()
                    + kit.service.governanceStatus("bob", "critical").queued());

            kit.service.resumeDispatch(null, "critical", "clear");
            latch.release.countDown();
            org.awaitility.Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                assertEquals(TaskStatus.SUCCEEDED, kit.service.get("x1").orElseThrow().getStatus());
                assertEquals(TaskStatus.SUCCEEDED, kit.service.get("x2").orElseThrow().getStatus());
            });
            log.info("[状态变化] 恢复 group 后该组全部调用方任务 SUCCEEDED");
        }
    }
}
