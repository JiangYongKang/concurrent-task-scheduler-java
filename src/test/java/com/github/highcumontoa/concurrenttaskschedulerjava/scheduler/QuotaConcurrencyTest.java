package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler;

import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.config.SchedulerProperties;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.QuotaVerdict;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.SubmitResult;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.TaskStatus;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 配额场景：并发度绝不越限；队列满时确定性拒绝；全局上限跨调用方生效；速率限制排队执行。
 */
class QuotaConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(QuotaConcurrencyTest.class);

    private SchedulerProperties smallProps() {
        SchedulerProperties p = TestEnv.defaultProps();
        p.setGlobalMaxConcurrency(4);
        p.setGlobalMaxQueued(100);
        p.setGlobalRateLimitPerSecond(1000);
        p.setDefaultMaxConcurrency(2);
        p.setDefaultMaxQueued(2);
        p.setDefaultRateLimitPerSecond(1000);
        return p;
    }

    @Test
    void callerConcurrencyNeverExceedsLimitAndExcessQueues() throws Exception {
        try (TestEnv env = TestEnv.create(smallProps())) {
            LatchHandler handler = env.registerLatch("batch");

            SubmitResult r1 = env.service.submit("team-a", "k1", "batch", null);
            SubmitResult r2 = env.service.submit("team-a", "k2", "batch", null);
            SubmitResult r3 = env.service.submit("team-a", "k3", "batch", null);
            SubmitResult r4 = env.service.submit("team-a", "k4", "batch", null);
            SubmitResult r5 = env.service.submit("team-a", "k5", "batch", null);

            log.info("[配额测试] r1={} r2={} r3={} r4={} r5={}",
                    r1.getVerdict(), r2.getVerdict(), r3.getVerdict(), r4.getVerdict(), r5.getVerdict());

            assertThat(handler.awaitEntered(500)).isTrue();

            // 调用方并发=2：前两个 RUNNING，之后两个排队，第 5 个因调用方队列上限 2 被拒绝
            assertThat(r1.getVerdict()).isEqualTo(QuotaVerdict.START);
            assertThat(r2.getVerdict()).isEqualTo(QuotaVerdict.START);
            assertThat(r3.getVerdict()).isEqualTo(QuotaVerdict.ENQUEUE);
            assertThat(r4.getVerdict()).isEqualTo(QuotaVerdict.ENQUEUE);
            assertThat(r5.getVerdict()).isEqualTo(QuotaVerdict.REJECT);
            assertThat(r5.getRejectReason()).isNotNull();
            assertThat(env.service.get(r5.getRecord().getTaskId()).getStatus())
                    .isEqualTo(TaskStatus.REJECTED);

            var quota = env.service.quota("team-a");
            log.info("[配额测试] 占用快照 running={}/{} queued={}/{}",
                    quota.getRunning(), quota.getMaxConcurrency(), quota.getQueued(), quota.getMaxQueued());
            assertThat(quota.getRunning()).isEqualTo(2);
            assertThat(quota.getQueued()).isEqualTo(2);

            // 放行后排队任务依次执行，最终全部完成，资源不泄漏
            handler.releaseAll();
            for (SubmitResult r : java.util.List.of(r1, r2, r3, r4)) {
                env.awaitStatus(r.getRecord().getTaskId(),
                        s -> s.equals("COMPLETED"), 5000);
            }
            Thread.sleep(100);
            var after = env.service.quota("team-a");
            log.info("[配额测试] 完成后快照 running={} queued={}", after.getRunning(), after.getQueued());
            assertThat(after.getRunning()).isZero();
            assertThat(after.getQueued()).isZero();
            assertThat(handler.invocations()).isEqualTo(4);
        }
    }

    @Test
    void globalConcurrencyCapAppliesAcrossCallers() throws Exception {
        SchedulerProperties p = smallProps();
        p.setDefaultMaxConcurrency(4); // 调用方上限放宽，使全局上限成为瓶颈
        try (TestEnv env = TestEnv.create(p)) {
            LatchHandler hA = env.registerLatch("batchA");
            env.registry.find("batchA");
            // 第二个调用方复用一个等价处理器需要不同 type：注册 batchB
            LatchHandler hB = env.registerLatch("batchB");

            for (int i = 0; i < 3; i++) {
                env.service.submit("team-a", "a" + i, "batchA", null);
            }
            for (int i = 0; i < 3; i++) {
                env.service.submit("team-b", "b" + i, "batchB", null);
            }

            assertThat(hA.awaitEntered(500)).isTrue();
            assertThat(hB.awaitEntered(500)).isTrue();
            Thread.sleep(80);

            var global = env.service.quota(null);
            log.info("[全局配额] running={}/{} queued={}/{}",
                    global.getRunning(), global.getMaxConcurrency(),
                    global.getQueued(), global.getMaxQueued());
            assertThat(global.getRunning()).isEqualTo(4);
            assertThat(global.getQueued()).isEqualTo(2);

            hA.releaseAll();
            hB.releaseAll();
            Thread.sleep(500);
            assertThat(env.service.quota(null).getRunning()).isZero();
            assertThat(env.service.quota(null).getQueued()).isZero();
        }
    }

    @Test
    void rateLimitThrottlesStartsButTasksEventuallyRun() throws Exception {
        SchedulerProperties p = TestEnv.defaultProps();
        p.setGlobalMaxConcurrency(10);
        p.setGlobalMaxQueued(100);
        p.setGlobalRateLimitPerSecond(2);
        p.setDefaultMaxConcurrency(10);
        p.setDefaultMaxQueued(100);
        p.setDefaultRateLimitPerSecond(1000);
        try (TestEnv env = TestEnv.create(p)) {
            LatchHandler handler = env.registerLatch("fast");

            long t0 = System.currentTimeMillis();
            var results = new java.util.ArrayList<SubmitResult>();
            for (int i = 0; i < 6; i++) {
                results.add(env.service.submit("team-r", "r" + i, "fast", null));
            }
            handler.releaseAll();

            // 全部最终完成；速率限制下令牌补充需要时间（初始 2 + 约 2/秒）
            for (SubmitResult r : results) {
                env.awaitStatus(r.getRecord().getTaskId(), s -> s.equals("COMPLETED"), 8000);
            }
            long elapsed = System.currentTimeMillis() - t0;
            log.info("[速率限制] 6 个任务全部完成耗时 {}ms（globalRate=2/s）", elapsed);
            assertThat(handler.invocations()).isEqualTo(6);
            // 初始桶 2 令牌 + 4 个需补充，理论上约需 ~2s
            assertThat(elapsed).isGreaterThan(1500);
        }
    }
}
