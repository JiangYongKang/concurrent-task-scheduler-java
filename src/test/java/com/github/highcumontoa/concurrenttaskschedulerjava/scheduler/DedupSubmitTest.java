package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler;

import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.config.SchedulerProperties;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.SubmitResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 重复提交场景：多线程并发以相同 (caller, submitKey) 提交，
 * 必须恰好创建一个任务、执行一次、占用一份配额；其余全部 duplicate=true 且任务 ID 相同。
 */
class DedupSubmitTest {

    private static final Logger log = LoggerFactory.getLogger(DedupSubmitTest.class);

    @Test
    void concurrentDuplicateSubmissionsExecuteExactlyOnce() throws Exception {
        SchedulerProperties p = TestEnv.defaultProps();
        p.setGlobalMaxConcurrency(8);
        p.setDefaultMaxConcurrency(8);
        p.setDefaultMaxQueued(8);
        try (TestEnv env = TestEnv.create(p)) {
            LatchHandler handler = env.registerLatch("idempotent");

            int threads = 20;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            ConcurrentLinkedQueue<SubmitResult> results = new ConcurrentLinkedQueue<>();
            CountDownLatch done = new CountDownLatch(threads);

            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        results.add(env.service.submit("team-d", "SAME-KEY-42", "idempotent", "p"));
                    } catch (Exception e) {
                        log.error("提交线程异常", e);
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
            pool.shutdown();

            List<SubmitResult> all = new ArrayList<>(results);
            long originals = all.stream().filter(r -> !r.isDuplicate()).count();
            long duplicates = all.stream().filter(SubmitResult::isDuplicate).count();
            long distinctTaskIds = all.stream().map(r -> r.getRecord().getTaskId()).distinct().count();

            log.info("[幂等] threads={} originals={} duplicates={} distinctTaskIds={}",
                    threads, originals, duplicates, distinctTaskIds);

            assertThat(originals).isEqualTo(1);
            assertThat(duplicates).isEqualTo(threads - 1);
            assertThat(distinctTaskIds).isEqualTo(1);

            handler.releaseAll();
            String taskId = all.get(0).getRecord().getTaskId();
            env.awaitStatus(taskId, s -> s.equals("COMPLETED"), 5000);

            Thread.sleep(100);
            log.info("[幂等] 处理器调用次数={}（必须为 1），caller running={} queued={}",
                    handler.invocations(),
                    env.service.quota("team-d").getRunning(),
                    env.service.quota("team-d").getQueued());
            assertThat(handler.invocations()).isEqualTo(1);
            assertThat(env.service.quota("team-d").getRunning()).isZero();
            assertThat(env.service.quota("team-d").getQueued()).isZero();
        }
    }

    @Test
    void duplicateWhileQueuedReturnsSameQueuedTaskAndNoExtraQuota() throws Exception {
        SchedulerProperties p = TestEnv.defaultProps();
        p.setGlobalMaxConcurrency(1);
        p.setDefaultMaxConcurrency(1);
        p.setDefaultMaxQueued(10);
        try (TestEnv env = TestEnv.create(p)) {
            LatchHandler handler = env.registerLatch("slow");

            SubmitResult first = env.service.submit("team-q", "q1", "slow", null);
            SubmitResult queued = env.service.submit("team-q", "q2", "slow", null);
            SubmitResult dup = env.service.submit("team-q", "q2", "slow", null);

            log.info("[幂等-排队] first={} queued={} dup.duplicate={} sameId={}",
                    first.getVerdict(), queued.getVerdict(), dup.isDuplicate(),
                    dup.getRecord().getTaskId().equals(queued.getRecord().getTaskId()));

            assertThat(queued.getVerdict().name()).isEqualTo("ENQUEUE");
            assertThat(dup.isDuplicate()).isTrue();
            assertThat(dup.getRecord().getTaskId()).isEqualTo(queued.getRecord().getTaskId());
            assertThat(env.service.quota("team-q").getQueued()).isEqualTo(1);

            handler.releaseAll();
            env.awaitStatus(queued.getRecord().getTaskId(), s -> s.equals("COMPLETED"), 5000);
            Thread.sleep(100);
            assertThat(handler.invocations()).isEqualTo(2);
        }
    }
}
