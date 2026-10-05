package com.github.highcumontoa.concurrenttaskschedulerjava.tests;

import com.github.highcumontoa.concurrenttaskschedulerjava.handler.TaskContext;
import com.github.highcumontoa.concurrenttaskschedulerjava.handler.TaskHandler;
import com.github.highcumontoa.concurrenttaskschedulerjava.model.RejectReason;
import com.github.highcumontoa.concurrenttaskschedulerjava.model.TaskRecord;
import com.github.highcumontoa.concurrenttaskschedulerjava.model.TaskStatus;
import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaStatus;
import com.github.highcumontoa.concurrenttaskschedulerjava.service.SubmitResult;
import com.github.highcumontoa.concurrenttaskschedulerjava.service.TaskSchedulerService;
import com.github.highcumontoa.concurrenttaskschedulerjava.tests.support.SchedulerTestKit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * 按调用方整体 / 按任务组整体的运行期治理测试：
 * 整体配额与暂停恢复、多作用域优先级与隔离、暂停状态下重启、
 * 非法作用域拒绝、并发提交与并发整体调整并存时上限不被突破且状态收敛。
 */
class ScopedGovernanceTests {

    private static final Logger log = LoggerFactory.getLogger(ScopedGovernanceTests.class);

    private static void awaitStarts(RuntimeGovernanceTests.GateHandler h, int n) {
        await().atMost(5, TimeUnit.SECONDS).until(() -> h.totalStarts.get() >= n);
    }

    private static void awaitStatus(TaskSchedulerService service, String taskId, TaskStatus s) {
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(
                () -> assertEquals(s, service.get(taskId).orElseThrow().getStatus(),
                        taskId + " 应收敛为 " + s));
    }

    /** 精确维度运行态计数必须与实际任务列表一致。 */
    private static void assertCountsConsistent(TaskSchedulerService service,
                                               String caller, String group) {
        QuotaStatus st = service.governanceStatus(caller, group);
        long queued = service.list().stream().filter(r -> r.getCallerId().equals(caller)
                && r.getGroup().equals(group)
                && (r.getStatus() == TaskStatus.QUEUED
                || r.getStatus() == TaskStatus.PENDING_RETRY)).count();
        long running = service.list().stream().filter(r -> r.getCallerId().equals(caller)
                && r.getGroup().equals(group)
                && r.getStatus() == TaskStatus.RUNNING).count();
        assertEquals(queued, st.queued(),
                "queued 计数必须与实际排队任务一致: " + st);
        assertEquals(running, st.active(),
                "active 计数必须与实际执行中任务一致: " + st);
        log.info("[运行态] scope={} caller={} group={} active={} queued={} 限额=({}/{}/{}) "
                        + "paused={} 最近操作={}",
                st.scope(), caller, group, st.active(), st.queued(),
                st.maxConcurrency(), st.rateLimitPerSecond(), st.maxQueued(),
                st.paused(), st.lastOperation());
    }

    /** 调用方整体运行态计数必须为该调用方下所有组实际任务的合计。 */
    private static void assertCallerCountsConsistent(TaskSchedulerService service,
                                                     String caller) {
        QuotaStatus st = service.governanceStatus(caller, null);
        assertEquals("CALLER", st.scope());
        long queued = service.list().stream().filter(r -> r.getCallerId().equals(caller)
                && (r.getStatus() == TaskStatus.QUEUED
                || r.getStatus() == TaskStatus.PENDING_RETRY)).count();
        long running = service.list().stream().filter(r -> r.getCallerId().equals(caller)
                && r.getStatus() == TaskStatus.RUNNING).count();
        assertEquals(queued, st.queued(), "调用方整体 queued 必须为各组合计: " + st);
        assertEquals(running, st.active(), "调用方整体 active 必须为各组合计: " + st);
        log.info("[运行态] scope=CALLER caller={} active={} queued={} 限额=({}/{}/{}) paused={}",
                caller, st.active(), st.queued(), st.maxConcurrency(),
                st.rateLimitPerSecond(), st.maxQueued(), st.paused());
    }

    // ---------------------------------------------------------------- 按调用方整体

    @Test
    void callerScopeQuotaAppliesToAllGroupsOfThatCallerOnly(@TempDir Path dir) {
        RuntimeGovernanceTests.GateHandler gate = new RuntimeGovernanceTests.GateHandler("gate");
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(8).concurrency(2).ratePerSecond(0).maxQueued(100)
                .timeout(20_000)
                .handler(() -> gate)
                .build()) {
            // alice 的两个组 + carol 的同名单组：默认每维度并发 2，全部打满
            for (String id : List.of("a1", "a2", "b1", "b2", "c1")) {
                gate.gateFor(id);
            }
            kit.service.submit("a1", "alice", "g1", "gate", "p", null);
            kit.service.submit("a2", "alice", "g1", "gate", "p", null);
            kit.service.submit("b1", "alice", "g2", "gate", "p", null);
            kit.service.submit("b2", "alice", "g2", "gate", "p", null);
            kit.service.submit("c1", "carol", "g1", "gate", "p", null);
            awaitStarts(gate, 5);
            log.info("[配额判定] 默认每维度并发 2：alice:g1/alice:g2/carol:g1 全部打满");

            // 不指定任务组：对 alice 整体调并发上限为 1
            QuotaStatus caller = kit.service.adjustQuota("alice", null, 1, 0, 100);
            assertEquals("CALLER", caller.scope());
            assertNull(caller.group());
            assertEquals(1, caller.maxConcurrency());
            assertEquals("ADJUST_QUOTA", caller.lastOperation());
            // 立即作用于 alice 下所有组；不影响 carol
            assertEquals(1, kit.service.governanceStatus("alice", "g1").maxConcurrency());
            assertEquals(1, kit.service.governanceStatus("alice", "g2").maxConcurrency());
            assertEquals(2, kit.service.governanceStatus("carol", "g1").maxConcurrency(),
                    "调用方整体调整不得串扰其它调用方");
            log.info("[治理] alice 整体并发调至 1，carol 保持默认 2");

            // 缩容不打断执行中任务；a1 完成后 alice:g1 active=1 等于新上限，新任务排队
            gate.open("a1");
            awaitStatus(kit.service, "a1", TaskStatus.SUCCEEDED);
            gate.gateFor("a3");
            assertTrue(kit.service.submit("a3", "alice", "g1", "gate", "p", null).accepted());
            sleep(200);
            assertEquals(5, gate.totalStarts.get(),
                    "alice:g1 active=1 达到整体新上限，a3 不得启动");
            // carol 不受 alice 整体调整影响：c2 照常启动
            gate.gateFor("c2");
            assertTrue(kit.service.submit("c2", "carol", "g1", "gate", "p", null).accepted());
            awaitStarts(gate, 6);
            log.info("[配额判定] a3 排队（alice 整体上限 1），carol 的 c2 照常启动");

            // 调用方整体运行态：各组计数合计
            assertCallerCountsConsistent(kit.service, "alice");
            QuotaStatus aliceAll = kit.service.governanceStatus("alice", null);
            assertEquals(3, aliceAll.active(), "alice 整体 active 应为 a2+b1+b2");
            assertEquals(1, aliceAll.queued(), "alice 整体 queued 应为 a3");
            assertCallerCountsConsistent(kit.service, "carol");

            gate.openAll();
            for (String id : List.of("a2", "a3", "b1", "b2", "c1", "c2")) {
                awaitStatus(kit.service, id, TaskStatus.SUCCEEDED);
            }
            assertCountsConsistent(kit.service, "alice", "g1");
            assertCountsConsistent(kit.service, "alice", "g2");
            assertCountsConsistent(kit.service, "carol", "g1");
            log.info("[状态变化] 调用方整体配额场景全部收敛 SUCCEEDED");
        }
    }

    @Test
    void callerScopePauseQueuesAllGroupsAndResumeRestoresOrder(@TempDir Path dir) {
        RuntimeGovernanceTests.GateHandler gate = new RuntimeGovernanceTests.GateHandler("gate");
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(8).concurrency(1).ratePerSecond(0).maxQueued(100)
                .timeout(20_000)
                .handler(() -> gate)
                .build()) {
            gate.gateFor("x1");
            kit.service.submit("x1", "alice", "g1", "gate", "p", null);
            awaitStarts(gate, 1);

            // 不指定任务组：暂停 alice 整体
            QuotaStatus paused = kit.service.pause("alice", null);
            assertEquals("CALLER", paused.scope());
            assertTrue(paused.paused());
            assertEquals("PAUSE", paused.lastOperation());
            log.info("[治理] 已暂停 alice 整体，执行中 x1 应自然跑完");

            // 暂停期间 alice 下两个组的新任务照常受理、进入排队，不拒收不丢
            for (String id : List.of("x2", "y1", "y2")) {
                gate.gateFor(id);
                SubmitResult r = kit.service.submit(id, "alice",
                        id.startsWith("x") ? "g1" : "g2", "gate", "p", null);
                assertTrue(r.accepted(), "暂停期间提交必须照常受理: " + id);
            }
            // 其它调用方不受影响：bob 的任务照常启动
            gate.gateFor("z1");
            assertTrue(kit.service.submit("z1", "bob", "g1", "gate", "p", null).accepted());
            awaitStarts(gate, 2);
            sleep(200);
            assertEquals(2, gate.totalStarts.get(), "暂停期间 alice 各组都不得启动新任务");
            assertEquals(TaskStatus.QUEUED, kit.service.get("x2").orElseThrow().getStatus());
            assertEquals(TaskStatus.QUEUED, kit.service.get("y1").orElseThrow().getStatus());
            assertEquals(TaskStatus.QUEUED, kit.service.get("y2").orElseThrow().getStatus());
            // 精确维度查询能看到有效暂停（继承自调用方整体），bob 维度不受影响
            assertTrue(kit.service.governanceStatus("alice", "g2").paused());
            assertFalse(kit.service.governanceStatus("bob", "g1").paused());
            assertCallerCountsConsistent(kit.service, "alice");
            log.info("[治理] alice 整体暂停中：x2/y1/y2 排队，bob 的 z1 照常执行");

            // 执行中任务自然跑完，暂停不补位
            gate.open("x1");
            awaitStatus(kit.service, "x1", TaskStatus.SUCCEEDED);
            sleep(200);
            assertEquals(2, gate.totalStarts.get(), "x1 完成后暂停仍然生效，不得补位");

            // 恢复后各组积压按原顺序继续（每维度上限 1，顺序确定）
            QuotaStatus resumed = kit.service.resume("alice", null);
            assertFalse(resumed.paused());
            assertEquals("RESUME", resumed.lastOperation());
            awaitStarts(gate, 4); // x2 (g1) 与 y1 (g2) 各放行队头
            assertEquals(TaskStatus.QUEUED, kit.service.get("y2").orElseThrow().getStatus(),
                    "g2 上限 1：y1 未完成前 y2 保持排队");
            gate.open("x2");
            awaitStatus(kit.service, "x2", TaskStatus.SUCCEEDED);
            gate.open("y1");
            awaitStatus(kit.service, "y1", TaskStatus.SUCCEEDED);
            awaitStarts(gate, 5); // y2 按序补位
            gate.open("y2");
            gate.open("z1");
            awaitStatus(kit.service, "y2", TaskStatus.SUCCEEDED);
            awaitStatus(kit.service, "z1", TaskStatus.SUCCEEDED);
            // 顺序断言：x1/z1 必在前两位（暂停前启动），y2 必在 y1 之后（同组 FIFO）
            assertEquals("x1", gate.startOrder.get(0));
            assertEquals("z1", gate.startOrder.get(1));
            assertEquals(new HashSet<>(List.of("x2", "y1")),
                    new HashSet<>(gate.startOrder.subList(2, 4)));
            assertTrue(gate.startOrder.indexOf("y1") < gate.startOrder.indexOf("y2"),
                    "同组积压必须按原顺序执行: " + gate.startOrder);
            assertCallerCountsConsistent(kit.service, "alice");
            log.info("[状态变化] 恢复后 alice 各组按序执行完成，顺序={}", gate.startOrder);
        }
    }

    // ---------------------------------------------------------------- 按任务组整体

    @Test
    void groupScopeGovernanceAppliesToAllCallersOfThatGroupOnly(@TempDir Path dir) {
        RuntimeGovernanceTests.GateHandler gate = new RuntimeGovernanceTests.GateHandler("gate");
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(8).concurrency(2).ratePerSecond(0).maxQueued(100)
                .timeout(20_000)
                .handler(() -> gate)
                .build()) {
            // 不指定调用方：对任务组 g2 整体调并发上限为 1
            QuotaStatus group = kit.service.adjustQuota(null, "g2", 1, 0, 100);
            assertEquals("GROUP", group.scope());
            assertNull(group.callerId());
            assertEquals(1, group.maxConcurrency());

            for (String id : List.of("p1", "p2", "q1", "r1", "r2")) {
                gate.gateFor(id);
            }
            kit.service.submit("p1", "alice", "g2", "gate", "p", null);
            kit.service.submit("p2", "alice", "g2", "gate", "p", null);
            kit.service.submit("q1", "bob", "g2", "gate", "p", null);
            kit.service.submit("r1", "alice", "g1", "gate", "p", null);
            kit.service.submit("r2", "alice", "g1", "gate", "p", null);
            // g2 对所有调用方限 1：p1、q1 启动，p2 排队；g1 不受组治理影响：r1、r2 启动
            awaitStarts(gate, 4);
            assertEquals(TaskStatus.QUEUED, kit.service.get("p2").orElseThrow().getStatus());
            assertEquals(1, kit.service.governanceStatus("bob", "g2").maxConcurrency());
            assertEquals(2, kit.service.governanceStatus("alice", "g1").maxConcurrency(),
                    "任务组整体调整不得串扰同调用方的其它组");
            log.info("[配额判定] g2 整体限 1（p1/q1 各启动，p2 排队），g1 保持默认 2");

            // 暂停 g2 整体：所有调用方的 g2 都停，g1 照常
            QuotaStatus paused = kit.service.pause(null, "g2");
            assertEquals("GROUP", paused.scope());
            assertTrue(paused.paused());
            gate.gateFor("p3");
            gate.gateFor("r3");
            assertTrue(kit.service.submit("p3", "alice", "g2", "gate", "p", null).accepted(),
                    "组暂停期间新任务照常受理排队");
            assertTrue(kit.service.submit("r3", "alice", "g1", "gate", "p", null).accepted());
            // g1 上限 2 被 r1/r2 占满：r3 先排队；放开 r1 后 r3 补位（组暂停不影响 g1）
            assertEquals(TaskStatus.QUEUED, kit.service.get("r3").orElseThrow().getStatus());
            gate.open("r1");
            awaitStatus(kit.service, "r1", TaskStatus.SUCCEEDED);
            awaitStarts(gate, 5); // r3 补位
            gate.open("p1");
            awaitStatus(kit.service, "p1", TaskStatus.SUCCEEDED);
            sleep(200);
            assertEquals(5, gate.totalStarts.get(), "g2 整体暂停：p1 完成后 p2 不得补位");
            QuotaStatus g2 = kit.service.governanceStatus(null, "g2");
            assertEquals("GROUP", g2.scope());
            assertTrue(g2.paused());
            assertEquals(2, g2.queued(), "g2 整体 queued 应为 p2+p3");
            assertEquals(1, g2.active(), "g2 整体 active 应为 q1");
            log.info("[治理] g2 整体暂停中：queued={} active={}（跨调用方合计）",
                    g2.queued(), g2.active());

            // 恢复后 g2 按各调用方维度继续，g1 全程未受影响
            kit.service.resume(null, "g2");
            awaitStarts(gate, 6); // p2 补位（alice:g2 队头）
            gate.openAll();
            for (String id : List.of("p2", "p3", "q1", "r2", "r3")) {
                awaitStatus(kit.service, id, TaskStatus.SUCCEEDED);
            }
            assertCountsConsistent(kit.service, "alice", "g2");
            assertCountsConsistent(kit.service, "bob", "g2");
            assertCountsConsistent(kit.service, "alice", "g1");
            log.info("[状态变化] 任务组整体治理场景全部收敛 SUCCEEDED");
        }
    }

    // ---------------------------------------------------------------- 优先级与隔离

    @Test
    void scopePrecedenceAndIsolation(@TempDir Path dir) {
        RuntimeGovernanceTests.GateHandler gate = new RuntimeGovernanceTests.GateHandler("gate");
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(8).concurrency(2).ratePerSecond(0).maxQueued(100)
                .timeout(20_000)
                .handler(() -> gate)
                .build()) {
            kit.service.adjustQuota("alice", null, 1, 0, 100);      // 调用方整体
            kit.service.adjustQuota("alice", "g1", 3, 0, 100);      // 精确维度
            kit.service.adjustQuota(null, "g2", 4, 0, 100);         // 任务组整体

            // 优先级：精确 > 调用方 > 任务组 > 默认
            assertEquals(3, kit.service.governanceStatus("alice", "g1").maxConcurrency(),
                    "精确维度覆盖优先于调用方整体");
            assertEquals(1, kit.service.governanceStatus("alice", "g2").maxConcurrency(),
                    "调用方整体覆盖优先于任务组整体");
            assertEquals(4, kit.service.governanceStatus("bob", "g2").maxConcurrency(),
                    "任务组整体覆盖作用于其它调用方");
            assertEquals(2, kit.service.governanceStatus("bob", "g1").maxConcurrency(),
                    "未覆盖维度回落默认值");
            assertEquals(2, kit.service.governanceStatus("carol", "g3").maxConcurrency());
            log.info("[治理] 优先级验证：alice:g1=3(精确) alice:g2=1(调用方) "
                    + "bob:g2=4(任务组) 其余=2(默认)");

            // 暂停隔离：精确暂停 alice:g1 不影响 alice 的其它组、也不影响其它调用方的 g1
            kit.service.pause("alice", "g1");
            gate.gateFor("ag1");
            gate.gateFor("ag2");
            gate.gateFor("bg1");
            kit.service.submit("ag1", "alice", "g1", "gate", "p", null);
            kit.service.submit("ag2", "alice", "g2", "gate", "p", null);
            kit.service.submit("bg1", "bob", "g1", "gate", "p", null);
            awaitStarts(gate, 2); // ag2、bg1 启动
            sleep(200);
            assertEquals(2, gate.totalStarts.get(), "精确暂停不得串扰其它维度");
            assertEquals(TaskStatus.QUEUED, kit.service.get("ag1").orElseThrow().getStatus());
            assertTrue(kit.service.governanceStatus("alice", "g1").paused());
            assertFalse(kit.service.governanceStatus("alice", "g2").paused());
            assertFalse(kit.service.governanceStatus("bob", "g1").paused());
            log.info("[治理] 精确暂停 alice:g1：ag1 排队，alice:g2 与 bob:g1 照常");

            kit.service.resume("alice", "g1");
            awaitStarts(gate, 3);
            gate.openAll();
            for (String id : List.of("ag1", "ag2", "bg1")) {
                awaitStatus(kit.service, id, TaskStatus.SUCCEEDED);
            }
            assertCountsConsistent(kit.service, "alice", "g1");
            assertCountsConsistent(kit.service, "alice", "g2");
            assertCountsConsistent(kit.service, "bob", "g1");
        }
    }

    // ---------------------------------------------------------------- 暂停状态下重启

    @Test
    void callerAndGroupScopeGovernanceSurviveRestart(@TempDir Path dir) {
        RuntimeGovernanceTests.GateHandler gate = new RuntimeGovernanceTests.GateHandler("gate");
        SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(8).concurrency(2).ratePerSecond(0).maxQueued(100)
                .timeout(20_000)
                .handler(() -> gate)
                .build();
        try {
            // 调用方整体暂停 + 任务组整体限 1
            kit.service.pause("alice", null);
            kit.service.adjustQuota(null, "g2", 1, 0, 50);
            for (String id : List.of("a1", "a2", "b1", "b2")) {
                gate.gateFor(id);
            }
            kit.service.submit("a1", "alice", "g1", "gate", "p", null);
            kit.service.submit("a2", "alice", "g2", "gate", "p", null);
            kit.service.submit("b1", "bob", "g2", "gate", "p", null);
            kit.service.submit("b2", "bob", "g2", "gate", "p", null);
            awaitStarts(gate, 1); // 仅 b1（bob:g2 限 1）
            assertEquals(2, kit.service.governanceStatus("alice", null).queued());
            assertEquals(2, kit.service.governanceStatus(null, "g2").queued());
            // 让 bob 的任务在重启前跑完，重启时只留 alice 的排队任务
            gate.open("b1");
            awaitStatus(kit.service, "b1", TaskStatus.SUCCEEDED);
            awaitStarts(gate, 2); // b2 补位
            gate.open("b2");
            awaitStatus(kit.service, "b2", TaskStatus.SUCCEEDED);
            log.info("[治理] 重启前：alice 整体暂停（a1/a2 排队），g2 整体限 1，bob 任务已完成");

            // 模拟进程重启
            kit = kit.restart();
            final SchedulerTestKit restarted = kit;

            // 重启后：调用方整体暂停与任务组整体配额都必须保留，不能自动放开
            QuotaStatus caller = restarted.service.governanceStatus("alice", null);
            assertEquals("CALLER", caller.scope());
            assertTrue(caller.paused(), "重启后调用方整体暂停必须保留");
            assertEquals(2, caller.queued(), "重启后排队任务不丢失");
            QuotaStatus group = restarted.service.governanceStatus(null, "g2");
            assertEquals("GROUP", group.scope());
            assertEquals(1, group.maxConcurrency(), "重启后任务组整体配额必须保留");
            assertEquals(50, group.maxQueued());
            assertEquals(TaskStatus.QUEUED, restarted.service.get("a1").orElseThrow().getStatus());
            assertEquals(TaskStatus.QUEUED, restarted.service.get("a2").orElseThrow().getStatus());
            sleep(200);
            assertEquals(2, gate.totalStarts.get(), "暂停的调用方重启后不得自动恢复放量");
            log.info("[治理] 重启后保持：alice 整体 paused=true，g2 整体限额=1/0/50");

            // 其它调用方在 g2 下仍受组配额约束：carol 提交两个，只放一个
            gate.gateFor("c1");
            gate.gateFor("c2");
            restarted.service.submit("c1", "carol", "g2", "gate", "p", null);
            restarted.service.submit("c2", "carol", "g2", "gate", "p", null);
            awaitStarts(gate, 3);
            assertEquals(TaskStatus.QUEUED, restarted.service.get("c2").orElseThrow().getStatus(),
                    "重启后 g2 整体限 1 对 carol 同样生效");
            gate.open("c1");
            awaitStatus(restarted.service, "c1", TaskStatus.SUCCEEDED);
            awaitStarts(gate, 4);
            gate.open("c2");
            awaitStatus(restarted.service, "c2", TaskStatus.SUCCEEDED);

            // 恢复 alice 整体：a1（g1，默认上限 2）与 a2（g2，组上限 1）放出
            restarted.service.resume("alice", null);
            awaitStarts(gate, 6);
            gate.openAll();
            awaitStatus(restarted.service, "a1", TaskStatus.SUCCEEDED);
            awaitStatus(restarted.service, "a2", TaskStatus.SUCCEEDED);
            assertCallerCountsConsistent(restarted.service, "alice");
            assertCountsConsistent(restarted.service, "alice", "g1");
            assertCountsConsistent(restarted.service, "alice", "g2");
            log.info("[状态变化] 重启恢复后 alice 积压任务执行完成，顺序={}", gate.startOrder);
        } finally {
            kit.close();
        }
    }

    // ---------------------------------------------------------------- 非法调整

    @Test
    void invalidScopedAdjustmentsRejectedAndEffectiveValuesUnchanged(@TempDir Path dir) {
        RuntimeGovernanceTests.GateHandler gate = new RuntimeGovernanceTests.GateHandler("gate");
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(2).concurrency(2).ratePerSecond(0).maxQueued(100)
                .handler(() -> gate)
                .build()) {
            // 基准：调用方整体合法调整
            kit.service.adjustQuota("alice", null, 1, 2, 30);

            // 缺少作用域信息：调用方和任务组都没给
            IllegalArgumentException e0 = assertThrows(IllegalArgumentException.class,
                    () -> kit.service.adjustQuota(null, null, 1, 1, 1));
            log.info("[治理] 缺少作用域的调整被拒绝: {}", e0.getMessage());
            assertThrows(IllegalArgumentException.class, () -> kit.service.pause(null, null));
            assertThrows(IllegalArgumentException.class, () -> kit.service.resume(null, null));
            assertThrows(IllegalArgumentException.class,
                    () -> kit.service.governanceStatus(null, null));
            // 空白字符串同样非法（null 才表示“不指定该维度”）
            assertThrows(IllegalArgumentException.class,
                    () -> kit.service.adjustQuota(" ", null, 1, 1, 1));
            assertThrows(IllegalArgumentException.class,
                    () -> kit.service.adjustQuota(null, " ", 1, 1, 1));
            assertThrows(IllegalArgumentException.class,
                    () -> kit.service.adjustQuota("alice", " ", 1, 1, 1));
            // 负数非法（调用方整体作用域）
            assertThrows(IllegalArgumentException.class,
                    () -> kit.service.adjustQuota("alice", null, -1, 2, 30));
            assertThrows(IllegalArgumentException.class,
                    () -> kit.service.adjustQuota(null, "g", 1, -1, 30));
            // 三项配额缺项非法
            assertThrows(IllegalArgumentException.class,
                    () -> kit.service.adjustQuota("alice", null, 1, null, 30));
            assertThrows(IllegalArgumentException.class,
                    () -> kit.service.adjustQuota(null, "g", null, 1, 30));

            QuotaStatus st = kit.service.governanceStatus("alice", null);
            assertEquals(1, st.maxConcurrency(), "非法调整后原并发上限必须保持不变");
            assertEquals(2, st.rateLimitPerSecond(), "非法调整后原速率上限必须保持不变");
            assertEquals(30, st.maxQueued(), "非法调整后原排队上限必须保持不变");
            assertFalse(st.paused());
            log.info("[治理] 非法调整后生效值保持: 并发={} 速率={} 排队={}",
                    st.maxConcurrency(), st.rateLimitPerSecond(), st.maxQueued());
        }
    }

    // ---------------------------------------------------------------- 并发提交 + 并发整体调整

    @Test
    void concurrentSubmitAndCallerScopeAdjustNeverBreachLimitsAndConverge(@TempDir Path dir)
            throws Exception {
        // 两个组各用一个处理器实例，分别统计并发峰值，便于精确断言整体上限
        class Work implements TaskHandler {
            private final String type;
            final AtomicInteger current = new AtomicInteger();
            final AtomicInteger maxSeen = new AtomicInteger();
            final AtomicInteger totalRuns = new AtomicInteger();

            Work(String type) {
                this.type = type;
            }

            @Override
            public String type() {
                return type;
            }

            @Override
            public String execute(String payload, TaskContext ctx) throws Exception {
                int now = current.incrementAndGet();
                maxSeen.accumulateAndGet(now, Math::max);
                totalRuns.incrementAndGet();
                try {
                    Thread.sleep(5 + Thread.currentThread().hashCode() % 3 * 5);
                } finally {
                    current.decrementAndGet();
                }
                return "ok";
            }
        }
        Work work1 = new Work("work1");
        Work work2 = new Work("work2");
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(8).concurrency(2).ratePerSecond(0).maxQueued(16)
                .timeout(20_000)
                .handler(() -> work1)
                .handler(() -> work2)
                .build()) {
            int threads = 4, perThread = 20;
            List<SubmitResult> results = Collections.synchronizedList(new ArrayList<>());
            List<Thread> submitters = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int tid = t;
                Thread th = new Thread(() -> {
                    for (int i = 0; i < perThread; i++) {
                        // 同一调用方 alice 的两个组交替提交
                        String group = (i % 2 == 0) ? "g1" : "g2";
                        String type = (i % 2 == 0) ? "work1" : "work2";
                        results.add(kit.service.submit("s-" + tid + "-" + i,
                                "alice", group, type, "p", null));
                    }
                }, "submitter-" + t);
                submitters.add(th);
                th.start();
            }
            // 并发整体调整线程：对 alice 整体反复调大调小，并穿插整体暂停/恢复
            Thread adjuster = new Thread(() -> {
                int[] limits = {1, 2, 3, 4};
                for (int i = 0; i < 25; i++) {
                    int limit = limits[i % limits.length];
                    kit.service.adjustQuota("alice", null, limit, 0, 64);
                    if (i % 8 == 4) {
                        kit.service.pause("alice", null);
                        sleep(30);
                        kit.service.resume("alice", null);
                    }
                    sleep(10);
                }
            }, "adjuster");
            adjuster.start();

            for (Thread th : submitters) {
                th.join();
            }
            adjuster.join();
            // 收尾：确保不处于暂停、给足上限，让所有已受理任务收敛
            kit.service.resume("alice", null);
            kit.service.adjustQuota("alice", null, 4, 0, 64);

            long accepted = results.stream().filter(SubmitResult::accepted).count();
            long rejected = results.stream().filter(r -> !r.accepted() && !r.duplicate()).count();
            assertEquals(threads * perThread, accepted + rejected,
                    "每个提交都必须有确定结果：接受或配额拒绝");
            results.stream().filter(r -> !r.accepted() && !r.duplicate()).forEach(r ->
                    assertEquals(RejectReason.QUOTA_EXHAUSTED, r.rejectReason(),
                            "并发整体调整期间唯一的拒绝原因必须是排队配额耗尽"));
            log.info("[提交结果] 接受={} 拒绝={} 总计={}", accepted, rejected, results.size());

            // 所有已受理任务最终收敛为 SUCCEEDED
            await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
                for (TaskRecord r : kit.service.list()) {
                    assertEquals(TaskStatus.SUCCEEDED, r.getStatus(),
                            "任务必须收敛为 SUCCEEDED: " + r.getTaskId());
                }
            });
            assertEquals(accepted, work1.totalRuns.get() + work2.totalRuns.get(),
                    "每个已受理任务恰好执行一次");
            assertTrue(work1.maxSeen.get() <= 4,
                    "g1 并发峰值不得超过调整过程中的最大生效上限 4, 实际=" + work1.maxSeen.get());
            assertTrue(work2.maxSeen.get() <= 4,
                    "g2 并发峰值不得超过调整过程中的最大生效上限 4, 实际=" + work2.maxSeen.get());
            log.info("[配额判定] 调用方整体调整期间峰值并发 g1={} g2={}（上限区间 1~4），总执行={}",
                    work1.maxSeen.get(), work2.maxSeen.get(),
                    work1.totalRuns.get() + work2.totalRuns.get());

            // 收敛后调用方整体运行态与实际一致：没有已结束任务残留在排队统计里
            QuotaStatus st = kit.service.governanceStatus("alice", null);
            assertEquals("CALLER", st.scope());
            assertEquals(0, st.active());
            assertEquals(0, st.queued());
            assertFalse(st.paused());
            assertCallerCountsConsistent(kit.service, "alice");
            assertCountsConsistent(kit.service, "alice", "g1");
            assertCountsConsistent(kit.service, "alice", "g2");
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
