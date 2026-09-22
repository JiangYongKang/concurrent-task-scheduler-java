package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler;

import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.config.SchedulerProperties;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.RetryPolicy;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.SubmitResult;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.TaskRecord;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler.TaskContext;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler.TaskHandler;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 重试场景：可重试错误按指数退避重试直到成功；超过最大尝试次数终态 FAILED 且原因可解释；
 * 不可重试错误立即终止，不产生多余副作用。
 */
class RetryTest {

    private static final Logger log = LoggerFactory.getLogger(RetryTest.class);

    @Test
    void backoffFormulaIsDeterministic() {
        RetryPolicy policy = new RetryPolicy(5, 100, 2.0, 1000);
        assertThat(policy.backoffMillis(1)).isEqualTo(100);
        assertThat(policy.backoffMillis(2)).isEqualTo(200);
        assertThat(policy.backoffMillis(3)).isEqualTo(400);
        assertThat(policy.backoffMillis(4)).isEqualTo(800);
        // 上限封顶
        RetryPolicy capped = new RetryPolicy(10, 100, 2.0, 500);
        assertThat(capped.backoffMillis(4)).isEqualTo(500);
    }

    @Test
    void retriesWithBackoffThenSucceeds() {
        try (TestEnv env = TestEnv.create(TestEnv.defaultProps())) {
            LatchHandler handler = env.registerLatch("flaky");
            handler.failFirst(2); // 前两次抛普通异常，第三次成功
            // LatchHandler 的失败路径不等待闸门；成功路径才等待，直接放行
            handler.releaseAll();

            SubmitResult r = env.service.submit("team-r", "retry-ok", "flaky", null);
            env.awaitStatus(r.getRecord().getTaskId(), s -> s.equals("COMPLETED"), 5000);

            TaskRecord rec = env.service.get(r.getRecord().getTaskId());
            log.info("[重试-成功] status={} attempts={} 明细={}",
                    rec.getStatus(), rec.getAttemptCount(),
                    rec.getAttempts().stream().map(a -> "#" + a.getAttemptNo() + ":" + (a.isSuccess() ? "OK" : a.getFailureCode())).toList());
            assertThat(rec.getStatus().name()).isEqualTo("COMPLETED");
            assertThat(rec.getAttemptCount()).isEqualTo(3);
            assertThat(handler.invocations()).isEqualTo(3);
            assertThat(rec.getAttempts()).hasSize(3);
            assertThat(rec.getAttempts().get(0).isSuccess()).isFalse();
            assertThat(rec.getAttempts().get(2).isSuccess()).isTrue();
        }
    }

    @Test
    void exhaustAttemptsFailsWithExplainableReason() {
        SchedulerProperties p = TestEnv.defaultProps();
        p.setDefaultMaxAttempts(2);
        try (TestEnv env = TestEnv.create(p)) {
            LatchHandler handler = env.registerLatch("alwaysFail");
            handler.failFirst(100);
            handler.releaseAll();

            SubmitResult r = env.service.submit("team-r", "retry-exhaust", "alwaysFail", null);
            env.awaitStatus(r.getRecord().getTaskId(), s -> s.equals("FAILED"), 5000);

            TaskRecord rec = env.service.get(r.getRecord().getTaskId());
            log.info("[重试-耗尽] status={} code={} message={}",
                    rec.getStatus(), rec.getFailure().getCode(), rec.getFailure().getMessage());
            assertThat(rec.getAttemptCount()).isEqualTo(2);
            assertThat(handler.invocations()).isEqualTo(2);
            assertThat(rec.getFailure().getCode().name()).isEqualTo("HANDLER_EXCEPTION");
            assertThat(rec.getFailure().isRetryable()).isFalse();
            assertThat(rec.getFailure().getMessage()).contains("最大尝试次数");
        }
    }

    @Test
    void nonRetryableErrorStopsImmediately() {
        try (TestEnv env = TestEnv.create(TestEnv.defaultProps())) {
            LatchHandler handler = env.registerLatch("dead");
            handler.failFirst(100).nonRetryable();
            handler.releaseAll();

            SubmitResult r = env.service.submit("team-r", "no-retry", "dead", null);
            env.awaitStatus(r.getRecord().getTaskId(), s -> s.equals("FAILED"), 5000);

            TaskRecord rec = env.service.get(r.getRecord().getTaskId());
            log.info("[不可重试] status={} code={} invocations={}",
                    rec.getStatus(), rec.getFailure().getCode(), handler.invocations());
            assertThat(rec.getAttemptCount()).isEqualTo(1);
            assertThat(handler.invocations()).isEqualTo(1);
            assertThat(rec.getFailure().getCode().name()).isEqualTo("NON_RETRYABLE");
        }
    }

    @Test
    void unknownTaskTypeIsRejectedAtSubmit() {
        try (TestEnv env = TestEnv.create(TestEnv.defaultProps())) {
            try {
                env.service.submit("team-r", "bad-type", "not-registered", null);
                throw new AssertionError("应抛出 IllegalArgumentException");
            } catch (IllegalArgumentException e) {
                log.info("[未知类型] 拒绝提交: {}", e.getMessage());
                assertThat(e.getMessage()).contains("未知 taskType");
            }
        }
    }

    /** 自定义非阻塞处理器，避免依赖 LatchHandler 的等待语义。 */
    @SuppressWarnings("unused")
    private static final class CountingHandler implements TaskHandler {
        final AtomicInteger calls = new AtomicInteger();
        final String type;

        CountingHandler(String type) {
            this.type = type;
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public void handle(String payload, TaskContext context) {
            calls.incrementAndGet();
        }
    }
}
