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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * 作用域级运行期治理测试：
 * <ul>
 *   <li>调用方级（只给 callerId）：一次对该调用方下全部任务组生效；</li>
 *   <li>任务组级（只给 group）：一次对该任务组下全部调用方生效；</li>
 *   <li>优先级 精确 &gt; 调用方级 &gt; 任务组级，且三级作用域互不串扰；</li>
 *   <li>作用域级暂停/配额覆盖跨进程重启保留；</li>
 *   <li>并发提交与并发作用域调整并存时上限不被突破、运行态计数一致。</li>
 * </ul>
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
        assertEquals(queued, st.queued(), "queued 计数必须与实际排队任务一致: " + st);
        assertEquals(running, st.active(), "active 计数必须与实际执行中任务一致: " + st);
        log.info("[运行态] scope={}:{} active={} queued={} 本秒启动={} 限额=({}/{}/{}) paused={}",
                caller, group, st.active(), st.queued(), st.startedInCurrentWindow(),
                st.maxConcurrency(), st.rateLimitPerSecond(), st.maxQueued(), st.paused());
    }

    /** 调用方级聚合视图：计数为该调用方全部任务组之和。 */
    private static void assertCallerViewConsistent(TaskSchedulerService service, String caller) {
        QuotaStatus st = service.governanceStatus(caller, null);
        assertNull(st.group(), "调用方级视图的 group 应为 null");
        long queued = service.list().stream().filter(r -> r.getCallerId().equals(caller)
                && (r.getStatus() == TaskStatus.QUEUED
                || r.getStatus() == TaskStatus.PENDING_RETRY)).count();
        long running = service.list().stream().filter(r -> r.getCallerId().equals(caller)
                && r.getStatus() == TaskStatus.RUNNING).count();
        assertEquals(queued, st.queued(), "调用方级 queued 必须为全组合计: " + st);
        assertEquals(running, st.active(), "调用方级 active 必须为全组合计: " + st);
        log.info("[运行态] 调用方级 caller={} active={} queued={} 限额=({}/{}/{}) paused={} 最近操作={}",
                caller, st.active(), st.queued(), st.maxConcurrency(),
                st.rateLimitPerSecond(), st.maxQueued(), st.paused(), st.lastOperation());
    }

    // ---------------------------------------------------------------- 调用方级整体治理

    @Test
    void callerLevelPauseAndQuotaAffectAllGroupsAndSurviveRestart(@TempDir Path dir) {
        RuntimeGovernanceTests.GateHandler gate = new RuntimeGovernanceTests.GateHandler("gate");
        SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(4).concurrency(2).ratePerSecond(0).maxQueued(100)
                .timeout(20_000)
                .handler(() -> gate)
                .build();
        try {
            // 两个组各跑一个执行中任务
            gate.gateFor("a1");
            gate.gateFor("b1");
            assertTrue(kit.service.submit("a1", "alice", "g1", "gate", "p", null).accepted());
            assertTrue(kit.service.submit("b1", "alice", "g2", "gate", "p", null).accepted());
            awaitStarts(gate, 2);

            // 不指定任务组：对整个调用方暂停 + 调配额（每组并发收紧为 1，顺序可确定性断言）
            QuotaStatus paused = kit.service.pause("alice", null);
            assertTrue(paused.paused());
            assertNull(paused.group(), "调用方级视图的 group 应为 null");
            assertEquals("PAUSE", paused.lastOperation());
            QuotaStatus adjusted = kit.service.adjustQuota("alice", null, 1, 0, 100);
            assertEquals(1, adjusted.maxConcurrency());
            log.info("[治理] 调用方级暂停+配额调整 caller=alice 生效于全部任务组");

            // 暂停期间：该调用方下所有组的新任务照常受理、进入排队，不拒收也不丢
            for (String id : List.of("a2", "a3")) {
                gate.gateFor(id);
                assertTrue(kit.service.submit(id, "alice", "g1", "gate", "p", null).accepted(),
                        "暂停期间提交必须照常受理: " + id);
            }
            gate.gateFor("b2");
            assertTrue(kit.service.submit("b2", "alice", "g2", "gate", "p", null).accepted());
            sleep(200);
            assertEquals(2, gate.totalStarts.get(), "暂停期间不得启动新任务");
            assertEquals(TaskStatus.QUEUED, kit.service.get("a2").orElseThrow().getStatus());
            assertEquals(TaskStatus.QUEUED, kit.service.get("a3").orElseThrow().getStatus());
            assertEquals(TaskStatus.QUEUED, kit.service.get("b2").orElseThrow().getStatus());

            // 执行中任务自然跑完（暂停不中断执行）
            gate.openAll();
            awaitStatus(kit.service, "a1", TaskStatus.SUCCEEDED);
            awaitStatus(kit.service, "b1", TaskStatus.SUCCEEDED);
            sleep(200);
            assertEquals(2, gate.totalStarts.get(), "执行中任务跑完后暂停仍生效，不得补位");

            // 调用方级运行态：聚合计数与暂停状态可见
            QuotaStatus callerView = kit.service.governanceStatus("alice", null);
            assertTrue(callerView.paused());
            assertEquals(3, callerView.queued(), "调用方级 queued 应为 g1(2)+g2(1) 合计");
            assertEquals(0, callerView.active());
            assertEquals(1, callerView.maxConcurrency(), "调用方级配额覆盖应在视图中可见");
            assertCallerViewConsistent(kit.service, "alice");
            assertCountsConsistent(kit.service, "alice", "g1");
            assertCountsConsistent(kit.service, "alice", "g2");

            // 模拟进程重启：调用方级暂停与配额覆盖都必须保留，不能自动放量
            kit = kit.restart();
            final SchedulerTestKit restarted = kit;
            QuotaStatus afterRestart = restarted.service.governanceStatus("alice", null);
            assertTrue(afterRestart.paused(), "重启后调用方级暂停必须保留");
            assertEquals(1, afterRestart.maxConcurrency(), "重启后调用方级配额覆盖必须保留");
            assertEquals(3, afterRestart.queued(), "重启后排队任务不丢失");
            assertEquals("ADJUST_QUOTA", afterRestart.lastOperation());
            sleep(200);
            assertEquals(2, gate.totalStarts.get(), "暂停的调用方重启后不得自动恢复放量");
            log.info("[治理] 重启后调用方级状态保持: paused=true 限额=1/0/100 queued=3");
            assertCallerViewConsistent(restarted.service, "alice");

            // 恢复后：积压任务按原来的顺序继续执行（每组上限 1，组内顺序确定）
            restarted.service.resume("alice", null);
            awaitStarts(gate, 4); // g1 放行 a2，g2 放行 b2
            awaitStatus(restarted.service, "a2", TaskStatus.SUCCEEDED);
            awaitStatus(restarted.service, "b2", TaskStatus.SUCCEEDED);
            awaitStarts(gate, 5); // a2 完成后 g1 才放行 a3
            awaitStatus(restarted.service, "a3", TaskStatus.SUCCEEDED);
            int a2Idx = gate.startOrder.indexOf("a2");
            int a3Idx = gate.startOrder.indexOf("a3");
            assertTrue(a2Idx >= 0 && a3Idx > a2Idx,
                    "恢复后同组积压必须按原顺序执行: " + gate.startOrder);
            assertCallerViewConsistent(restarted.service, "alice");
            assertCountsConsistent(restarted.service, "alice", "g1");
            assertCountsConsistent(restarted.service, "alice", "g2");
            log.info("[状态变化] 调用方级恢复后全部收敛 SUCCEEDED，顺序={}", gate.startOrder);
        } finally {
            kit.close();
        }
    }

    @Test
    void callerLevelQuotaLimitsEveryGroupAndExactOverrideWins(@TempDir Path dir) {
        RuntimeGovernanceTests.GateHandler gate = new RuntimeGovernanceTests.GateHandler("gate");
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(4).concurrency(2).ratePerSecond(0).maxQueued(100)
                .timeout(20_000)
                .handler(() -> gate)
                .build()) {
            // 调用方级并发上限 1：对 alice 下每个组生效
            kit.service.adjustQuota("alice", null, 1, 0, 100);
            gate.gateFor("a1");
            gate.gateFor("a2");
            gate.gateFor("b1");
            assertTrue(kit.service.submit("a1", "alice", "g1", "gate", "p", null).accepted());
            assertTrue(kit.service.submit("a2", "alice", "g1", "gate", "p", null).accepted());
            assertTrue(kit.service.submit("b1", "alice", "g2", "gate", "p", null).accepted());
            awaitStarts(gate, 2); // g1 放行 a1，g2 放行 b1（每组各 1）
            sleep(200);
            assertEquals(2, gate.totalStarts.get(), "调用方级上限 1 时每组最多放行 1 个");
            assertEquals(TaskStatus.QUEUED, kit.service.get("a2").orElseThrow().getStatus());
            log.info("[配额判定] 调用方级上限 1：g1 的 a2 排队，g1/g2 各跑 1 个");

            // 精确调整 alice:g1 为 2：只影响 g1，不影响 g2（互不串扰，精确 > 调用方级）
            kit.service.adjustQuota("alice", "g1", 2, 0, 100);
            awaitStarts(gate, 3); // g1 上限变 2，a2 放出
            assertEquals(TaskStatus.RUNNING, kit.service.get("a2").orElseThrow().getStatus());
            assertEquals(2, kit.service.governanceStatus("alice", "g1").maxConcurrency());
            assertEquals(1, kit.service.governanceStatus("alice", "g2").maxConcurrency(),
                    "精确调整 g1 不得改动 g2 的生效值（仍为调用方级覆盖 1）");
            log.info("[配额判定] 精确调整 alice:g1=2 后 a2 放出，g2 仍受调用方级上限 1 约束");

            gate.openAll();
            awaitStatus(kit.service, "a1", TaskStatus.SUCCEEDED);
            awaitStatus(kit.service, "a2", TaskStatus.SUCCEEDED);
            awaitStatus(kit.service, "b1", TaskStatus.SUCCEEDED);
            assertCallerViewConsistent(kit.service, "alice");
        }
    }

    // ---------------------------------------------------------------- 任务组级整体治理

    @Test
    void groupLevelGovernanceAffectsAllCallersOnly(@TempDir Path dir) {
        RuntimeGovernanceTests.GateHandler gate = new RuntimeGovernanceTests.GateHandler("gate");
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(4).concurrency(2).ratePerSecond(0).maxQueued(100)
                .timeout(20_000)
                .handler(() -> gate)
                .build()) {
            // 只给任务组、不带调用方：对 shared 组的全部调用方生效
            QuotaStatus adjusted = kit.service.adjustQuota(null, "shared", 1, 0, 100);
            assertNull(adjusted.callerId(), "任务组级视图的 callerId 应为 null");
            assertEquals(1, adjusted.maxConcurrency());

            gate.gateFor("x1");
            gate.gateFor("x2");
            gate.gateFor("y1");
            assertTrue(kit.service.submit("x1", "alice", "shared", "gate", "p", null).accepted());
            assertTrue(kit.service.submit("x2", "alice", "shared", "gate", "p", null).accepted());
            assertTrue(kit.service.submit("y1", "bob", "shared", "gate", "p", null).accepted());
            awaitStarts(gate, 2); // alice:shared 放行 x1，bob:shared 放行 y1
            sleep(200);
            assertEquals(2, gate.totalStarts.get(), "任务组级上限 1 时每个调用方维度最多放行 1 个");
            assertEquals(TaskStatus.QUEUED, kit.service.get("x2").orElseThrow().getStatus());
            log.info("[配额判定] 任务组级上限 1：alice 的 x2 排队，alice/bob 各跑 1 个");

            // 任务组级暂停：alice 与 bob 的 shared 组都不再启动新任务
            QuotaStatus paused = kit.service.pause(null, "shared");
            assertTrue(paused.paused());
            gate.gateFor("y2");
            assertTrue(kit.service.submit("y2", "bob", "shared", "gate", "p", null).accepted(),
                    "任务组级暂停期间新任务照常受理排队");
            // 同一调用方的其它组不受任务组级暂停影响（互不串扰）
            gate.gateFor("z1");
            assertTrue(kit.service.submit("z1", "alice", "other", "gate", "p", null).accepted());
            awaitStarts(gate, 3); // z1 立即启动
            sleep(200);
            assertEquals(3, gate.totalStarts.get(), "shared 组暂停期间不得放行，other 组不受影响");
            assertEquals(TaskStatus.QUEUED, kit.service.get("y2").orElseThrow().getStatus());
            assertEquals(TaskStatus.RUNNING, kit.service.get("z1").orElseThrow().getStatus());
            log.info("[治理] 任务组级暂停 shared：alice/bob 的 shared 排队，alice:other 照常执行");

            // 任务组级运行态：跨调用方合计
            QuotaStatus groupView = kit.service.governanceStatus(null, "shared");
            assertTrue(groupView.paused());
            assertEquals(2, groupView.active(), "组级 active 为 alice(1)+bob(1) 合计");
            assertEquals(2, groupView.queued(), "组级 queued 为 x2+y2 合计");
            log.info("[运行态] 任务组级 group=shared active={} queued={} paused={}",
                    groupView.active(), groupView.queued(), groupView.paused());

            // 恢复后全部收敛
            kit.service.resume(null, "shared");
            gate.openAll();
            for (String id : List.of("x1", "x2", "y1", "y2", "z1")) {
                awaitStatus(kit.service, id, TaskStatus.SUCCEEDED);
            }
            assertCountsConsistent(kit.service, "alice", "shared");
            assertCountsConsistent(kit.service, "bob", "shared");
            assertCountsConsistent(kit.service, "alice", "other");
        }
    }

    // ---------------------------------------------------------------- 优先级与互不串扰

    @Test
    void scopePrecedenceAndIsolation(@TempDir Path dir) {
        RuntimeGovernanceTests.GateHandler gate = new RuntimeGovernanceTests.GateHandler("gate");
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(4).concurrency(2).ratePerSecond(0).maxQueued(100)
                .timeout(20_000)
                .handler(() -> gate)
                .build()) {
            kit.service.adjustQuota(null, "g", 4, 0, 40);       // 任务组级
            kit.service.adjustQuota("alice", null, 3, 0, 30);   // 调用方级
            kit.service.adjustQuota("alice", "g", 1, 0, 10);    // 精确

            // 生效优先级：精确 > 调用方级 > 任务组级 > 默认
            QuotaStatus exact = kit.service.governanceStatus("alice", "g");
            assertEquals(1, exact.maxConcurrency());
            assertEquals(10, exact.maxQueued());
            QuotaStatus callerOnly = kit.service.governanceStatus("alice", "other");
            assertEquals(3, callerOnly.maxConcurrency(), "alice:other 应生效调用方级覆盖");
            assertEquals(30, callerOnly.maxQueued());
            QuotaStatus groupOnly = kit.service.governanceStatus("bob", "g");
            assertEquals(4, groupOnly.maxConcurrency(), "bob:g 应生效任务组级覆盖");
            assertEquals(40, groupOnly.maxQueued());
            QuotaStatus untouched = kit.service.governanceStatus("bob", "other");
            assertEquals(2, untouched.maxConcurrency(), "bob:other 应保持默认");
            assertEquals(100, untouched.maxQueued());
            log.info("[配额判定] 优先级验证: 精确(1/0/10) > 调用方级(3/0/30) > 任务组级(4/0/40) > 默认(2/0/100)");

            // 精确暂停 alice:g：不得串扰同调用方的其它组、同组的其它调用方
            kit.service.pause("alice", "g");
            gate.gateFor("e1");
            gate.gateFor("s1");
            gate.gateFor("s2");
            assertTrue(kit.service.submit("e1", "alice", "g", "gate", "p", null).accepted());
            assertTrue(kit.service.submit("s1", "alice", "other", "gate", "p", null).accepted());
            assertTrue(kit.service.submit("s2", "bob", "g", "gate", "p", null).accepted());
            awaitStarts(gate, 2); // s1（同 caller 其它组）与 s2（同组其它 caller）照常启动
            sleep(200);
            assertEquals(2, gate.totalStarts.get());
            assertEquals(TaskStatus.QUEUED, kit.service.get("e1").orElseThrow().getStatus(),
                    "精确暂停的维度不得启动新任务");
            assertEquals(TaskStatus.RUNNING, kit.service.get("s1").orElseThrow().getStatus());
            assertEquals(TaskStatus.RUNNING, kit.service.get("s2").orElseThrow().getStatus());
            assertTrue(kit.service.governanceStatus("alice", "g").paused());
            assertFalse(kit.service.governanceStatus("alice", "other").paused(),
                    "精确暂停不得串扰同一调用方下的其它组");
            assertFalse(kit.service.governanceStatus("bob", "g").paused(),
                    "精确暂停不得串扰同一任务组下的其它调用方");
            log.info("[治理] 精确暂停 alice:g 不串扰 alice:other 与 bob:g");

            // 精确维度的行为上限也按精确覆盖 1 判定：e1 恢复后单独放行
            kit.service.resume("alice", "g");
            awaitStarts(gate, 3);
            gate.openAll();
            for (String id : List.of("e1", "s1", "s2")) {
                awaitStatus(kit.service, id, TaskStatus.SUCCEEDED);
            }
            assertCountsConsistent(kit.service, "alice", "g");
            assertCountsConsistent(kit.service, "alice", "other");
            assertCountsConsistent(kit.service, "bob", "g");
        }
    }

    @Test
    void missingScopeIsRejectedAndEffectiveValuesUnchanged(@TempDir Path dir) {
        RuntimeGovernanceTests.GateHandler gate = new RuntimeGovernanceTests.GateHandler("gate");
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(2).concurrency(2).ratePerSecond(0).maxQueued(100)
                .handler(() -> gate)
                .build()) {
            kit.service.adjustQuota("alice", null, 3, 0, 30);
            kit.service.pause("alice", null);

            // 调用方和任务组都没给：缺少作用域信息，拒绝且保持原状态
            assertThrows(IllegalArgumentException.class,
                    () -> kit.service.adjustQuota(null, null, 1, 1, 1));
            assertThrows(IllegalArgumentException.class,
                    () -> kit.service.adjustQuota("", null, 1, 1, 1));
            assertThrows(IllegalArgumentException.class,
                    () -> kit.service.pause(null, null));
            assertThrows(IllegalArgumentException.class,
                    () -> kit.service.resume(" ", " "));
            // 作用域级同样拒绝负值与缺项
            assertThrows(IllegalArgumentException.class,
                    () -> kit.service.adjustQuota("alice", null, -1, 0, 30));
            assertThrows(IllegalArgumentException.class,
                    () -> kit.service.adjustQuota(null, "g", 1, null, 1));

            QuotaStatus st = kit.service.governanceStatus("alice", null);
            assertEquals(3, st.maxConcurrency(), "非法调整后调用方级原值必须保持不变");
            assertEquals(30, st.maxQueued());
            assertTrue(st.paused(), "非法暂停/恢复请求不得改动原暂停状态");
            log.info("[治理] 缺少作用域/负值/缺项均被拒绝，原状态保持: 限额=3/0/30 paused=true");
        }
    }

    // ---------------------------------------------------------------- 并发提交 + 并发作用域调整

    @Test
    void concurrentSubmitAndScopedAdjustNeverBreachLimitsAndConverge(@TempDir Path dir)
            throws Exception {
        AtomicInteger current = new AtomicInteger();
        AtomicInteger maxSeen = new AtomicInteger();
        AtomicInteger totalRuns = new AtomicInteger();
        TaskHandler work = new TaskHandler() {
            @Override
            public String type() {
                return "work";
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
        };
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(8).concurrency(2).ratePerSecond(0).maxQueued(16)
                .timeout(20_000)
                .handler(() -> work)
                .build()) {
            int threads = 4, perThread = 20;
            List<SubmitResult> results = Collections.synchronizedList(new ArrayList<>());
            List<Thread> submitters = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int tid = t;
                Thread th = new Thread(() -> {
                    for (int i = 0; i < perThread; i++) {
                        results.add(kit.service.submit("s-" + tid + "-" + i,
                                "alice", "g", "work", "p", null));
                    }
                }, "submitter-" + t);
                submitters.add(th);
                th.start();
            }
            // 并发调整线程：在调用方级与任务组级作用域上反复调大调小，并穿插暂停/恢复
            Thread adjuster = new Thread(() -> {
                int[] limits = {1, 2, 3, 4};
                for (int i = 0; i < 25; i++) {
                    int limit = limits[i % limits.length];
                    if (i % 2 == 0) {
                        kit.service.adjustQuota("alice", null, limit, 0, 64);
                    } else {
                        kit.service.adjustQuota(null, "g", limit, 0, 64);
                    }
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
            // 收尾：清除所有作用域的暂停并给足上限，让已受理任务收敛
            kit.service.resume("alice", null);
            kit.service.resume(null, "g");
            kit.service.adjustQuota("alice", null, 4, 0, 64);

            long accepted = results.stream().filter(SubmitResult::accepted).count();
            long rejected = results.stream().filter(r -> !r.accepted() && !r.duplicate()).count();
            assertEquals(threads * perThread, accepted + rejected,
                    "每个提交都必须有确定结果：接受或配额拒绝");
            results.stream().filter(r -> !r.accepted() && !r.duplicate()).forEach(r ->
                    assertEquals(RejectReason.QUOTA_EXHAUSTED, r.rejectReason(),
                            "并发调整期间唯一的拒绝原因必须是排队配额耗尽"));
            log.info("[提交结果] 接受={} 拒绝={} 总计={}", accepted, rejected, results.size());

            await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
                for (TaskRecord r : kit.service.list()) {
                    assertEquals(TaskStatus.SUCCEEDED, r.getStatus(),
                            "任务必须收敛为 SUCCEEDED: " + r.getTaskId());
                }
            });
            assertEquals(accepted, totalRuns.get(), "每个已受理任务恰好执行一次");
            assertTrue(maxSeen.get() <= 4,
                    "并发峰值不得超过调整过程中的最大生效上限 4, 实际=" + maxSeen.get());
            log.info("[配额判定] 作用域级并发调整期间峰值并发={}（上限区间 1~4），总执行={}",
                    maxSeen.get(), totalRuns.get());

            // 收敛后运行态与实际一致：没有已拒/已结束任务残留在排队统计里
            QuotaStatus callerView = kit.service.governanceStatus("alice", null);
            assertEquals(0, callerView.active());
            assertEquals(0, callerView.queued());
            assertFalse(callerView.paused());
            assertCallerViewConsistent(kit.service, "alice");
            assertCountsConsistent(kit.service, "alice", "g");
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
