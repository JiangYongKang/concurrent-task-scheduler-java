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
 * 重启恢复场景：
 * 1) 排队任务重启后继续执行，不丢失；
 * 2) RUNNING 任务重启默认判 FAILED(RESTART_INTERRUPTED)，配额不泄漏；
 *    开启 retry-running-tasks-on-restart 后按 WAITING_RETRY 退避重试；
 * 3) WAITING_RETRY 任务重启后按原 nextRunAt 继续；
 * 4) 终态（COMPLETED/FAILED/CANCELLED/REJECTED）重启后不回退。
 */
class RestartRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(RestartRecoveryTest.class);

    private SchedulerProperties restartProps() {
        SchedulerProperties p = TestEnv.defaultProps();
        p.setGlobalMaxConcurrency(1);
        p.setDefaultMaxConcurrency(1);
        p.setDefaultMaxQueued(10);
        return p;
    }

    @Test
    void queuedTaskSurvivesRestartAndEventuallyCompletes() throws Exception {
        SchedulerProperties p = restartProps();
        java.nio.file.Path dir;
        String queuedId;
        TestEnv env = TestEnv.create(p);
        dir = env.dataDir;
        LatchHandler running = env.registerLatch("busy");
        env.registerLatch("waiting");
        env.service.submit("team-s", "s1", "busy", null);
        SubmitResult queued = env.service.submit("team-s", "s2", "waiting", null);
        assertThat(running.awaitEntered(500)).isTrue();
        assertThat(queued.getVerdict().name()).isEqualTo("ENQUEUE");
        queuedId = queued.getRecord().getTaskId();
        env.kill(); // s1 运行中、s2 排队时进程被杀死

        try (TestEnv env2 = TestEnv.open(restartProps(), dir)) {
            // 恢复后：s1(RUNNING) 判失败；s2(QUEUED) 重新排队，现在没有竞争者可立即执行
            env2.registerLatch("busy").releaseAll();
            LatchHandler waiting2 = env2.registerLatch("waiting");
            waiting2.releaseAll();
            env2.awaitStatus(queuedId, s -> s.equals("COMPLETED"), 5000);
            TaskRecord rec = env2.service.get(queuedId);
            log.info("[重启-排队续跑] status={} attempts={}", rec.getStatus(), rec.getAttemptCount());
            assertThat(rec.getStatus()).isEqualTo(TaskStatus.COMPLETED);
        }
    }

    @Test
    void runningTaskFailsOnRestartByDefaultAndQuotaIsClean() throws Exception {
        SchedulerProperties p = restartProps();
        java.nio.file.Path dir;
        String runId;
        try (TestEnv env = TestEnv.create(p)) {
            dir = env.dataDir;
            // 直接植入 RUNNING 记录，等价于执行中进程被 kill -9
            runId = env.seedRunningTask("team-s", "run1", "work", 1);
            env.kill();
        }

        try (TestEnv env2 = TestEnv.open(restartProps(), dir)) {
            env2.registerLatch("work").releaseAll();
            TaskRecord rec = env2.service.get(runId);
            log.info("[重启-运行中断] status={} code={}", rec.getStatus(), rec.getFailure().getCode());
            assertThat(rec.getStatus()).isEqualTo(TaskStatus.FAILED);
            assertThat(rec.getFailure().getCode().name()).isEqualTo("RESTART_INTERRUPTED");

            // 崩溃进程的槽位全部归零，配额不泄漏
            assertThat(env2.service.quota(null).getRunning()).isZero();
            assertThat(env2.service.quota("team-s").getRunning()).isZero();

            // 名额可复用：新任务能立即启动
            LatchHandler h2 = env2.registerLatch("work2");
            SubmitResult r2 = env2.service.submit("team-s", "run2", "work2", null);
            assertThat(r2.getVerdict().name()).isEqualTo("START");
            assertThat(h2.awaitEntered(500)).isTrue();
            h2.releaseAll();
            env2.awaitStatus(r2.getRecord().getTaskId(), s -> s.equals("COMPLETED"), 3000);
        }
    }

    @Test
    void runningTaskRetriesAfterRestartWhenConfigured() throws Exception {
        SchedulerProperties p = restartProps();
        p.setRetryRunningTasksOnRestart(true);
        java.nio.file.Path dir;
        String taskId;
        try (TestEnv env = TestEnv.create(p)) {
            dir = env.dataDir;
            taskId = env.seedRunningTask("team-s", "runR", "work", 1);
            env.kill();
        }

        SchedulerProperties p2 = restartProps();
        p2.setRetryRunningTasksOnRestart(true);
        try (TestEnv env2 = TestEnv.open(p2, dir)) {
            env2.registerLatch("work").releaseAll();
            env2.awaitStatus(taskId, s -> s.equals("COMPLETED"), 5000);
            TaskRecord rec = env2.service.get(taskId);
            log.info("[重启-重试] status={} attempts={}", rec.getStatus(), rec.getAttemptCount());
            assertThat(rec.getStatus()).isEqualTo(TaskStatus.COMPLETED);
            assertThat(rec.getAttemptCount()).isGreaterThanOrEqualTo(2);
        }
    }

    @Test
    void terminalStatesDoNotRollbackAfterRestart() throws Exception {
        SchedulerProperties p = restartProps();
        // team-s 队列容量 0：并发被占后再提交即 REJECTED；team-z 使用默认队列容量
        SchedulerProperties.CallerQuota teamS = new SchedulerProperties.CallerQuota();
        teamS.setMaxQueued(0);
        p.getCallers().put("team-s", teamS);
        java.nio.file.Path dir;
        String completedId;
        String rejectedId;
        String cancelledId;
        try (TestEnv env = TestEnv.create(p)) {
            dir = env.dataDir;
            LatchHandler ok = env.registerLatch("ok");
            ok.releaseAll();
            SubmitResult c = env.service.submit("team-s", "done", "ok", null);
            completedId = c.getRecord().getTaskId();
            env.awaitStatus(completedId, s -> s.equals("COMPLETED"), 3000);

            LatchHandler busy = env.registerLatch("busy");
            env.service.submit("team-s", "occupy", "busy", null);
            assertThat(busy.awaitEntered(300)).isTrue();
            SubmitResult rej = env.service.submit("team-s", "rej", "busy", null);
            assertThat(rej.getVerdict().name()).isEqualTo("REJECT");
            rejectedId = rej.getRecord().getTaskId();
            assertThat(env.service.get(rejectedId).getStatus()).isEqualTo(TaskStatus.REJECTED);
            busy.releaseAll();
            env.awaitStatus(env.service.getBySubmitKey("team-s", "occupy").getTaskId(),
                    s -> s.equals("COMPLETED"), 3000);

            // 另一调用方：制造一个排队后取消的终态
            LatchHandler busy2 = env.registerLatch("busy2");
            SubmitResult zq = env.service.submit("team-z", "zq", "busy2", null);
            assertThat(zq.getVerdict().name()).isEqualTo("START");
            assertThat(busy2.awaitEntered(300)).isTrue();
            SubmitResult q2 = env.service.submit("team-z", "zq2", "busy2", null);
            assertThat(q2.getVerdict().name()).isEqualTo("ENQUEUE");
            cancelledId = q2.getRecord().getTaskId();
            env.service.cancel(cancelledId);
            env.awaitStatus(cancelledId, s -> s.equals("CANCELLED"), 2000);
            busy2.releaseAll();
            log.info("[重启-终态] 重启前 completed={} rejected={} cancelled={}",
                    completedId, rejectedId, cancelledId);
        }

        SchedulerProperties p2 = restartProps();
        p2.getCallers().put("team-s", teamS);
        try (TestEnv env2 = TestEnv.open(p2, dir)) {
            assertThat(env2.service.get(completedId).getStatus()).isEqualTo(TaskStatus.COMPLETED);
            assertThat(env2.service.get(rejectedId).getStatus()).isEqualTo(TaskStatus.REJECTED);
            assertThat(env2.service.get(cancelledId).getStatus()).isEqualTo(TaskStatus.CANCELLED);
            // 终态任务不占用任何恢复计数
            assertThat(env2.service.quota(null).getRunning()).isZero();
            assertThat(env2.service.quota(null).getQueued()).isZero();
            log.info("[重启-终态] 三种终态重启后均不回退");
        }
    }

    @Test
    void waitingRetryResumesAfterRestart() throws Exception {
        SchedulerProperties p = restartProps();
        p.setDefaultMaxAttempts(3);
        java.nio.file.Path dir;
        String taskId;
        try (TestEnv env = TestEnv.create(p)) {
            dir = env.dataDir;
            LatchHandler flaky = env.registerLatch("flaky");
            flaky.failFirst(100);
            flaky.releaseAll();
            SubmitResult r = env.service.submit("team-s", "wr", "flaky", null);
            taskId = r.getRecord().getTaskId();
            // 第一次失败后进入 WAITING_RETRY 时停止
            env.awaitStatus(taskId, s -> s.equals("WAITING_RETRY"), 3000);
            env.kill(); // 进程被杀死
        }

        SchedulerProperties p2 = restartProps();
        p2.setDefaultMaxAttempts(3);
        try (TestEnv env2 = TestEnv.open(p2, dir)) {
            // 处理器仍然失败：应从第 2 次尝试继续直到耗尽 -> FAILED，共 3 次调用
            LatchHandler flaky2 = env2.registerLatch("flaky");
            flaky2.failFirst(100);
            flaky2.releaseAll();
            env2.awaitStatus(taskId, s -> s.equals("FAILED"), 5000);
            TaskRecord rec = env2.service.get(taskId);
            log.info("[重启-重试等待] status={} attempts={}", rec.getStatus(), rec.getAttemptCount());
            assertThat(rec.getAttemptCount()).isEqualTo(3);
            assertThat(flaky2.invocations()).isEqualTo(2); // 重启前已用掉第 1 次
        }
    }
}
