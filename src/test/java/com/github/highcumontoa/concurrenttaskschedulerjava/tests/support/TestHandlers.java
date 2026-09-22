package com.github.highcumontoa.concurrenttaskschedulerjava.tests.support;

import com.github.highcumontoa.concurrenttaskschedulerjava.handler.TaskContext;
import com.github.highcumontoa.concurrenttaskschedulerjava.handler.TaskHandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/** 测试夹具：可精确控制开始/放行/计数的处理器，避免依赖 sleep 做断言。 */
public final class TestHandlers {

    private TestHandlers() {
    }

    /** 每次开始执行 countDown(started)，并阻塞在 release 上；可观测最大并发。 */
    public static final class LatchHandler implements TaskHandler {
        public final CountDownLatch started;
        public final CountDownLatch release = new CountDownLatch(1);
        public final AtomicInteger current = new AtomicInteger();
        public final AtomicInteger maxSeen = new AtomicInteger();
        public final AtomicInteger totalRuns = new AtomicInteger();
        private final String type;

        public LatchHandler(String type, int expectedStarts) {
            this.type = type;
            this.started = new CountDownLatch(expectedStarts);
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
            started.countDown();
            try {
                while (release.getCount() > 0) {
                    if (ctx.cancelled()) {
                        throw new InterruptedException("cancelled");
                    }
                    release.await(20, java.util.concurrent.TimeUnit.MILLISECONDS);
                }
            } finally {
                current.decrementAndGet();
            }
            return "done";
        }
    }

    /** 前 failTimes 次执行抛异常，之后成功；fatal=true 时抛不可重试异常。 */
    public static final class FlakyHandler implements TaskHandler {
        public final AtomicInteger runs = new AtomicInteger();
        private final String type;
        private final int failTimes;
        private final boolean fatal;

        public FlakyHandler(String type, int failTimes, boolean fatal) {
            this.type = type;
            this.failTimes = failTimes;
            this.fatal = fatal;
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public String execute(String payload, TaskContext ctx) {
            int n = runs.incrementAndGet();
            if (n <= failTimes) {
                if (fatal) {
                    throw new com.github.highcumontoa.concurrenttaskschedulerjava.retry.NonRetryableTaskException("boom-" + n);
                }
                throw new IllegalStateException("boom-" + n);
            }
            return "ok-after-" + (n - 1);
        }
    }

    /** 忽略中断标记的处理器：验证超时“隔离”——即使任务不响应中断也不会被重复执行。 */
    public static final class IgnoringInterruptHandler implements TaskHandler {
        public final AtomicInteger runs = new AtomicInteger();
        private final String type;
        private final long busyMillis;

        public IgnoringInterruptHandler(String type, long busyMillis) {
            this.type = type;
            this.busyMillis = busyMillis;
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public String execute(String payload, TaskContext ctx) {
            runs.incrementAndGet();
            long deadline = System.currentTimeMillis() + busyMillis;
            // 主动吞掉中断，模拟不协作任务
            while (System.currentTimeMillis() < deadline) {
                Thread.interrupted();
            }
            return "finished";
        }
    }
}
