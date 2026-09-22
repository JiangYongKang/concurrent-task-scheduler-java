package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler;

import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.config.SchedulerProperties;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.SubmitResult;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.TaskRecord;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.TaskStatus;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 取消/超时场景：
 * 排队任务取消 -> CANCELLED 且队列名额释放（后续任务可补位）；
 * 运行中任务取消 -> 协作式停止，CANCELLED，无重试；
 * 执行超时 -> TIMED_OUT 终态，线程被中断，不重试。
 */
class CancelTimeoutTest {

    private static final Logger log = LoggerFactory.getLogger(CancelTimeoutTest.class);

    @Test
    void queuedTaskCancellationReleasesQueueSlot() throws Exception {
        SchedulerProperties p = TestEnv.defaultProps();
        p.setGlobalMaxConcurrency(1);
        p.setDefaultMaxConcurrency(1);
        p.setDefaultMaxQueued(1);
        p.setDefaultRateLimitPerSecond(1000);
        p.setGlobalRateLimitPerSecond(1000);
        try (TestEnv env = TestEnv.create(p)) {
            LatchHandler running = env.registerLatch("busy");
            LatchHandler waiting = env.registerLatch("next");

            SubmitResult r1 = env.service.submit("team-c", "c1", "busy", null);
            SubmitResult r2 = env.service.submit("team-c", "c2", "next", null);
            SubmitResult r3 = env.service.submit("team-c", "c3", "next", null);

            assertThat(running.awaitEntered(500)).isTrue();
            assertThat(r2.getVerdict().name()).isEqualTo("ENQUEUE");
            // 调用方队列上限=1，c3 被拒绝
            assertThat(r3.getVerdict().name()).isEqualTo("REJECT");
            log.info("[取消-排队] c1={} c2={} c3={}", r1.getVerdict(), r2.getVerdict(), r3.getVerdict());

            TaskRecord cancelled = env.service.cancel(r2.getRecord().getTaskId());
            assertThat(cancelled.getStatus()).isEqualTo(TaskStatus.CANCELLED);
            assertThat(cancelled.getFailure().getCode().name()).isEqualTo("CANCELLED");

            // 队列名额释放后新提交可以排队
            SubmitResult r4 = env.service.submit("team-c", "c4", "next", null);
            log.info("[取消-排队] 名额释放后 c4 verdict={}", r4.getVerdict());
            assertThat(r4.getVerdict().name()).isEqualTo("ENQUEUE");

            running.releaseAll();
            waiting.releaseAll();
            env.awaitStatus(r4.getRecord().getTaskId(), s -> s.equals("COMPLETED"), 5000);
            Thread.sleep(100);
            assertThat(env.service.quota("team-c").getQueued()).isZero();
        }
    }

    @Test
    void runningTaskCancellationStopsCooperatively() throws Exception {
        try (TestEnv env = TestEnv.create(TestEnv.defaultProps())) {
            LatchHandler handler = env.registerLatch("coop").cooperative();
            SubmitResult r = env.service.submit("team-x", "x1", "coop", null);
            assertThat(handler.awaitEntered(500)).isTrue();

            TaskRecord before = env.service.cancel(r.getRecord().getTaskId());
            assertThat(before.getStatus()).isEqualTo(TaskStatus.RUNNING);

            env.awaitStatus(r.getRecord().getTaskId(), s -> s.equals("CANCELLED"), 3000);
            TaskRecord after = env.service.get(r.getRecord().getTaskId());
            log.info("[取消-运行] 最终状态={} failureCode={} attempts={}",
                    after.getStatus(), after.getFailure().getCode(), after.getAttemptCount());
            assertThat(after.getStatus()).isEqualTo(TaskStatus.CANCELLED);
            assertThat(after.getFailure().getCode().name()).isEqualTo("CANCELLED");
            assertThat(handler.invocations()).isEqualTo(1); // 不重试
        }
    }

    @Test
    void attemptTimeoutTerminatesTaskWithoutRetry() throws Exception {
        SchedulerProperties p = TestEnv.defaultProps();
        p.setAttemptTimeoutMillis(150);
        try (TestEnv env = TestEnv.create(p)) {
            // 非协作（不检查取消）的阻塞处理器：超时也应通过中断/看门狗终态化
            LatchHandler handler = env.registerLatch("stuck");
            SubmitResult r = env.service.submit("team-t", "t1", "stuck", null);
            assertThat(handler.awaitEntered(500)).isTrue();

            env.awaitStatus(r.getRecord().getTaskId(), s -> s.equals("TIMED_OUT"), 3000);
            TaskRecord rec = env.service.get(r.getRecord().getTaskId());
            log.info("[超时] 状态={} failureCode={} message={} attempts={}",
                    rec.getStatus(), rec.getFailure().getCode(),
                    rec.getFailure().getMessage(), rec.getAttemptCount());
            assertThat(rec.getStatus()).isEqualTo(TaskStatus.TIMED_OUT);
            assertThat(rec.getFailure().getCode().name()).isEqualTo("ATTEMPT_TIMEOUT");
            assertThat(rec.getAttemptCount()).isEqualTo(1); // 超时不重试

            // 超时释放的并发名额必须可被新任务复用
            LatchHandler h2 = env.registerLatch("after");
            SubmitResult r2 = env.service.submit("team-t", "t2", "after", null);
            assertThat(r2.getVerdict().name()).isEqualTo("START");
            assertThat(h2.awaitEntered(500)).isTrue();
            h2.releaseAll();
            env.awaitStatus(r2.getRecord().getTaskId(), s -> s.equals("COMPLETED"), 3000);
        }
    }
}
