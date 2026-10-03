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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * 运行期治理测试：配额热调整（缩容不打断/扩容放积压/非法拒绝）、
 * 暂停与恢复、暂停状态下重启、并发提交与并发调整并存时上限不被突破且状态收敛。
 */
class RuntimeGovernanceTests {

    private static final Logger log = LoggerFactory.getLogger(RuntimeGovernanceTests.class);

    /** 可按 taskId 逐个放行的处理器：记录启动顺序与最大并发，便于确定性断言。 */
    static final class GateHandler implements TaskHandler {
        private final String type;
        final Map<String, CountDownLatch> gates = new ConcurrentHashMap<>();
        final List<String> startOrder = new CopyOnWriteArrayList<>();
        final AtomicInteger current = new AtomicInteger();
        final AtomicInteger maxSeen = new AtomicInteger();
        final AtomicInteger totalStarts = new AtomicInteger();

        GateHandler(String type) {
            this.type = type;
        }

        CountDownLatch gateFor(String taskId) {
            return gates.computeIfAbsent(taskId, k -> new CountDownLatch(1));
        }

        void open(String taskId) {
            gateFor(taskId).countDown();
        }

        void openAll() {
            gates.values().forEach(CountDownLatch::countDown);
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public String execute(String payload, TaskContext ctx) throws Exception {
            int now = current.incrementAndGet();
            maxSeen.accumulateAndGet(now, Math::max);
            totalStarts.incrementAndGet();
            startOrder.add(ctx.taskId());
            log.info("[状态变化] taskId={} 开始执行 当前并发={} 历史峰值={}",
                    ctx.taskId(), now, maxSeen.get());
            CountDownLatch gate = gateFor(ctx.taskId());
            try {
                long deadline = System.currentTimeMillis() + 15_000;
                while (gate.getCount() > 0) {
                    if (ctx.cancelled()) {
                        throw new InterruptedException("cancelled");
                    }
                    if (System.currentTimeMillis() > deadline) {
                        throw new IllegalStateException("gate not opened in time: " + ctx.taskId());
                    }
                    gate.await(20, TimeUnit.MILLISECONDS);
                }
            } finally {
                current.decrementAndGet();
            }
            return "done:" + ctx.taskId();
        }
    }

    private static void awaitStarts(GateHandler h, int n) {
        await().atMost(5, TimeUnit.SECONDS).until(() -> h.totalStarts.get() >= n);
    }

    private static void awaitStatus(TaskSchedulerService service, String taskId, TaskStatus s) {
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(
                () -> assertEquals(s, service.get(taskId).orElseThrow().getStatus(),
                        taskId + " 应收敛为 " + s));
    }

    /** 运行态计数必须与实际任务列表一致：已拒绝/已结束的任务不得留在排队统计里。 */
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
        log.info("[运行态] caller={} group={} active={} queued={} 本秒启动={} 限额=({}/{}/{}) "
                        + "paused={} 最近操作={}@{}",
                caller, group, st.active(), st.queued(), st.startedInCurrentWindow(),
                st.maxConcurrency(), st.rateLimitPerSecond(), st.maxQueued(),
                st.paused(), st.lastOperation(), st.lastOperationAtEpochMillis());
    }

    // ---------------------------------------------------------------- 配额热调整

    @Test
    void shrinkConcurrencyDoesNotInterruptRunningAndStopsNewStarts(@TempDir Path dir) {
        GateHandler gate = new GateHandler("gate");
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(4).concurrency(2).ratePerSecond(0).maxQueued(100)
                .timeout(20_000)
                .handler(() -> gate)
                .build()) {
            for (int i = 1; i <= 4; i++) {
                String id = "g" + i;
                gate.gateFor(id);
                assertTrue(kit.service.submit(id, "alice", "g", "gate", "p", null).accepted());
            }
            awaitStarts(gate, 2);
            log.info("[配额判定] 默认上限 2：g1/g2 执行中，g3/g4 排队");

            // 运行期把并发从 2 缩到 1：执行中任务不得被中断、状态不得回退
            QuotaStatus st = kit.service.adjustQuota("alice", "g", 1, 0, 100);
            assertEquals(1, st.maxConcurrency());
            assertEquals("ADJUST_QUOTA", st.lastOperation());
            assertTrue(st.lastOperationAtEpochMillis() > 0);
            assertEquals(TaskStatus.RUNNING, kit.service.get("g1").orElseThrow().getStatus());
            assertEquals(TaskStatus.RUNNING, kit.service.get("g2").orElseThrow().getStatus());
            assertEquals(TaskStatus.QUEUED, kit.service.get("g3").orElseThrow().getStatus());

            // 缩容后 active=2 > 新上限 1：不得再放行新任务
            sleep(200);
            assertEquals(2, gate.totalStarts.get(), "缩容后 active 超过新上限时不得再启动");
            assertCountsConsistent(kit.service, "alice", "g");

            // 放行 g1：active 降到 1，仍等于新上限，g3 不得启动
            gate.open("g1");
            awaitStatus(kit.service, "g1", TaskStatus.SUCCEEDED);
            sleep(200);
            assertEquals(2, gate.totalStarts.get(),
                    "active=1 等于新上限 1 时不得再启动, starts=" + gate.totalStarts.get());
            log.info("[配额判定] 缩容到 1 后 active=1 达到新上限，g3 保持排队");

            // 放行 g2：active 降到 0，g3 按顺序启动
            gate.open("g2");
            awaitStatus(kit.service, "g2", TaskStatus.SUCCEEDED);
            awaitStarts(gate, 3);
            assertEquals("g3", gate.startOrder.get(2), "空出槽位后必须按原顺序放出 g3");
            gate.open("g3");
            awaitStatus(kit.service, "g3", TaskStatus.SUCCEEDED);
            awaitStarts(gate, 4);
            gate.open("g4");
            awaitStatus(kit.service, "g4", TaskStatus.SUCCEEDED);
            assertEquals(2, gate.maxSeen.get(), "整个过程中并发峰值不得超过旧上限 2");
            assertCountsConsistent(kit.service, "alice", "g");
            log.info("[状态变化] 缩容场景全部收敛 SUCCEEDED，峰值并发={}", gate.maxSeen.get());
        }
    }

    @Test
    void growConcurrencyReleasesBacklogInOriginalOrder(@TempDir Path dir) {
        GateHandler gate = new GateHandler("gate");
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(4).concurrency(1).ratePerSecond(0).maxQueued(100)
                .timeout(20_000)
                .handler(() -> gate)
                .build()) {
            for (int i = 1; i <= 4; i++) {
                String id = "b" + i;
                gate.gateFor(id);
                assertTrue(kit.service.submit(id, "alice", "g", "gate", "p", null).accepted());
            }
            awaitStarts(gate, 1);
            assertEquals(TaskStatus.QUEUED, kit.service.get("b2").orElseThrow().getStatus());

            // 运行期把并发从 1 调到 3：积压任务立即按原顺序放出
            QuotaStatus st = kit.service.adjustQuota("alice", "g", 3, 0, 100);
            assertEquals(3, st.maxConcurrency());
            awaitStarts(gate, 3);
            assertEquals(List.of("b1", "b2", "b3"), gate.startOrder,
                    "扩容后必须按入队顺序放出积压任务");
            assertEquals(TaskStatus.QUEUED, kit.service.get("b4").orElseThrow().getStatus(),
                    "新上限 3 用满后 b4 仍排队");
            log.info("[配额判定] 扩容 1->3 后按序放出 b2/b3，b4 等待空槽");
            assertCountsConsistent(kit.service, "alice", "g");

            gate.openAll();
            for (int i = 1; i <= 4; i++) {
                awaitStatus(kit.service, "b" + i, TaskStatus.SUCCEEDED);
            }
            assertEquals(4, gate.totalStarts.get(), "每个任务恰好执行一次");
            assertTrue(gate.maxSeen.get() <= 3, "并发峰值不得超过新上限 3");
            assertCountsConsistent(kit.service, "alice", "g");
        }
    }

    @Test
    void invalidAdjustmentIsRejectedAndEffectiveValueUnchanged(@TempDir Path dir) {
        GateHandler gate = new GateHandler("gate");
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(2).concurrency(2).ratePerSecond(0).maxQueued(100)
                .handler(() -> gate)
                .build()) {
            // 先做一次合法调整作为基准
            kit.service.adjustQuota("alice", "g", 2, 5, 50);

            // 负值非法：拒绝且保持原值
            IllegalArgumentException e1 = assertThrows(IllegalArgumentException.class,
                    () -> kit.service.adjustQuota("alice", "g", -1, 5, 50));
            log.info("[治理] 非法调整被拒绝: {}", e1.getMessage());
            assertThrows(IllegalArgumentException.class,
                    () -> kit.service.adjustQuota("alice", "g", 2, -5, 50));
            assertThrows(IllegalArgumentException.class,
                    () -> kit.service.adjustQuota("alice", "g", 2, 5, -50));
            // 缺项非法
            assertThrows(IllegalArgumentException.class,
                    () -> kit.service.adjustQuota("alice", "g", null, 5, 50));
            // 空调用方非法
            assertThrows(IllegalArgumentException.class,
                    () -> kit.service.adjustQuota(" ", "g", 1, 1, 1));

            QuotaStatus st = kit.service.governanceStatus("alice", "g");
            assertEquals(2, st.maxConcurrency(), "非法调整后原并发上限必须保持不变");
            assertEquals(5, st.rateLimitPerSecond(), "非法调整后原速率上限必须保持不变");
            assertEquals(50, st.maxQueued(), "非法调整后原排队上限必须保持不变");
            assertEquals("ADJUST_QUOTA", st.lastOperation());
            log.info("[治理] 非法调整后生效值保持: 并发={} 速率={} 排队={}",
                    st.maxConcurrency(), st.rateLimitPerSecond(), st.maxQueued());
        }
    }

    // ---------------------------------------------------------------- 暂停 / 恢复

    @Test
    void pauseKeepsAcceptingAndQueuingAndResumeRestoresOrder(@TempDir Path dir) {
        GateHandler gate = new GateHandler("gate");
        try (SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(4).concurrency(2).ratePerSecond(0).maxQueued(100)
                .timeout(20_000)
                .handler(() -> gate)
                .build()) {
            // 先跑一个执行中任务，验证暂停期间执行中任务自然跑完
            gate.gateFor("p0");
            assertTrue(kit.service.submit("p0", "alice", "g", "gate", "p", null).accepted());
            awaitStarts(gate, 1);

            QuotaStatus paused = kit.service.pause("alice", "g");
            assertTrue(paused.paused());
            assertEquals("PAUSE", paused.lastOperation());
            log.info("[治理] 已暂停 alice:g，执行中 p0 应自然跑完");

            // 暂停期间新提交照常受理、进入排队，不拒绝不丢失
            for (int i = 1; i <= 3; i++) {
                String id = "p" + i;
                gate.gateFor(id);
                SubmitResult r = kit.service.submit(id, "alice", "g", "gate", "p", null);
                assertTrue(r.accepted(), "暂停期间提交必须照常受理: " + id);
            }
            sleep(200);
            assertEquals(1, gate.totalStarts.get(), "暂停期间不得启动新任务");
            for (int i = 1; i <= 3; i++) {
                assertEquals(TaskStatus.QUEUED,
                        kit.service.get("p" + i).orElseThrow().getStatus(),
                        "暂停期间任务必须保持排队: p" + i);
            }
            QuotaStatus st = kit.service.governanceStatus("alice", "g");
            assertTrue(st.paused());
            assertEquals(3, st.queued());
            assertEquals(1, st.active());
            assertCountsConsistent(kit.service, "alice", "g");

            // 执行中任务自然跑完（暂停不中断执行）
            gate.open("p0");
            awaitStatus(kit.service, "p0", TaskStatus.SUCCEEDED);
            sleep(200);
            assertEquals(1, gate.totalStarts.get(), "p0 完成后暂停仍然生效，不得补位");

            // 恢复后按原公平顺序继续执行
            QuotaStatus resumed = kit.service.resume("alice", "g");
            assertFalse(resumed.paused());
            assertEquals("RESUME", resumed.lastOperation());
            awaitStarts(gate, 3); // 上限 2：p1、p2 启动
            gate.openAll();
            for (int i = 1; i <= 3; i++) {
                awaitStatus(kit.service, "p" + i, TaskStatus.SUCCEEDED);
            }
            assertEquals(List.of("p0", "p1", "p2", "p3"), gate.startOrder,
                    "恢复后必须按原入队顺序执行");
            assertCountsConsistent(kit.service, "alice", "g");
            log.info("[状态变化] 恢复后 p1~p3 按序执行完成，顺序={}", gate.startOrder);
        }
    }

    @Test
    void pauseAndQuotaAdjustmentSurviveRestart(@TempDir Path dir) {
        GateHandler gate = new GateHandler("gate");
        SchedulerTestKit kit = SchedulerTestKit.builder(dir)
                .workers(4).concurrency(2).ratePerSecond(0).maxQueued(100)
                .timeout(20_000)
                .handler(() -> gate)
                .build();
        try {
            // 暂停 + 运行期配额调整，然后提交两个任务（暂停期间照常排队）
            kit.service.pause("alice", "g");
            kit.service.adjustQuota("alice", "g", 5, 0, 50);
            gate.gateFor("q1");
            gate.gateFor("q2");
            assertTrue(kit.service.submit("q1", "alice", "g", "gate", "p", null).accepted());
            assertTrue(kit.service.submit("q2", "alice", "g", "gate", "p", null).accepted());
            assertEquals(2, kit.service.governanceStatus("alice", "g").queued());

            // 模拟进程重启
            kit = kit.restart();
            final SchedulerTestKit restarted = kit;

            // 重启后：暂停状态与配额调整都必须保留，不能自动恢复放量
            QuotaStatus st = restarted.service.governanceStatus("alice", "g");
            assertTrue(st.paused(), "重启后暂停状态必须保留");
            assertEquals(5, st.maxConcurrency(), "重启后运行期配额调整必须保留");
            assertEquals(0, st.rateLimitPerSecond());
            assertEquals(50, st.maxQueued());
            assertEquals(2, st.queued(), "重启后排队任务不丢失");
            assertEquals("ADJUST_QUOTA", st.lastOperation(), "最近治理操作必须随重启保留");
            assertTrue(st.lastOperationAtEpochMillis() > 0);
            assertEquals(TaskStatus.QUEUED, restarted.service.get("q1").orElseThrow().getStatus());
            assertEquals(TaskStatus.QUEUED, restarted.service.get("q2").orElseThrow().getStatus());
            sleep(200);
            assertEquals(0, gate.totalStarts.get(), "暂停维度重启后不得自动恢复放量");
            log.info("[治理] 重启后暂停与配额调整保持: paused=true 限额=5/0/50 queued=2");
            assertCountsConsistent(restarted.service, "alice", "g");

            // 恢复后排队任务继续执行
            restarted.service.resume("alice", "g");
            awaitStarts(gate, 2);
            gate.openAll();
            awaitStatus(restarted.service, "q1", TaskStatus.SUCCEEDED);
            awaitStatus(restarted.service, "q2", TaskStatus.SUCCEEDED);
            assertEquals(List.of("q1", "q2"), gate.startOrder);
            assertCountsConsistent(restarted.service, "alice", "g");
            log.info("[状态变化] 重启恢复后 q1/q2 按序执行完成");
        } finally {
            kit.close();
        }
    }

    // ---------------------------------------------------------------- 并发提交 + 并发调整

    @Test
    void concurrentSubmitAndAdjustNeverBreachLimitsAndConverge(@TempDir Path dir)
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
            // 并发调整线程：在合法范围内反复调大调小，并穿插短暂暂停/恢复
            Thread adjuster = new Thread(() -> {
                int[] limits = {1, 2, 3, 4};
                for (int i = 0; i < 25; i++) {
                    int limit = limits[i % limits.length];
                    kit.service.adjustQuota("alice", "g", limit, 0, 64);
                    if (i % 8 == 4) {
                        kit.service.pause("alice", "g");
                        sleep(30);
                        kit.service.resume("alice", "g");
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
            kit.service.resume("alice", "g");
            kit.service.adjustQuota("alice", "g", 4, 0, 64);

            long accepted = results.stream().filter(SubmitResult::accepted).count();
            long rejected = results.stream().filter(r -> !r.accepted() && !r.duplicate()).count();
            assertEquals(threads * perThread, accepted + rejected,
                    "每个提交都必须有确定结果：接受或配额拒绝");
            results.stream().filter(r -> !r.accepted() && !r.duplicate()).forEach(r ->
                    assertEquals(RejectReason.QUOTA_EXHAUSTED, r.rejectReason(),
                            "并发调整期间唯一的拒绝原因必须是排队配额耗尽"));
            log.info("[提交结果] 接受={} 拒绝={} 总计={}", accepted, rejected, results.size());

            // 所有已受理任务最终收敛为 SUCCEEDED
            await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
                for (TaskRecord r : kit.service.list()) {
                    assertEquals(TaskStatus.SUCCEEDED, r.getStatus(),
                            "任务必须收敛为 SUCCEEDED: " + r.getTaskId());
                }
            });
            assertEquals(accepted, totalRuns.get(), "每个已受理任务恰好执行一次");
            assertTrue(maxSeen.get() <= 4,
                    "并发峰值不得超过调整过程中的最大生效上限 4, 实际=" + maxSeen.get());
            log.info("[配额判定] 并发调整期间峰值并发={}（上限区间 1~4），总执行={}",
                    maxSeen.get(), totalRuns.get());

            // 收敛后运行态与实际一致：没有已结束任务残留在排队统计里
            QuotaStatus st = kit.service.governanceStatus("alice", "g");
            assertEquals(0, st.active());
            assertEquals(0, st.queued());
            assertFalse(st.paused());
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
