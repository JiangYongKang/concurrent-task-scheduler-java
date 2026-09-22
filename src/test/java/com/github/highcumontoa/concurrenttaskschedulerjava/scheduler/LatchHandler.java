package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler;

import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler.NonRetryableTaskException;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler.TaskContext;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler.TaskHandler;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试用处理器：
 * <ul>
 *   <li>进入 handle 时 countEntered，然后等待 release 闸门（或超时/取消退出）；</li>
 *   <li>支持「前 N 次失败」模式验证重试；</li>
 *   <li>支持「不可重试异常」模式；</li>
 *   <li>取消时响应中断/context.isCancelled()，及时退出。</li>
 * </ul>
 */
public class LatchHandler implements TaskHandler {

    private final String type;
    private final CountDownLatch entered = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private final AtomicInteger invocations = new AtomicInteger();
    private volatile int failFirstN;
    private volatile boolean nonRetryable;
    private volatile boolean checkCancellation;

    public LatchHandler(String type) {
        this.type = type;
    }

    @Override
    public String type() {
        return type;
    }

    public LatchHandler failFirst(int n) {
        this.failFirstN = n;
        return this;
    }

    public LatchHandler nonRetryable() {
        this.nonRetryable = true;
        return this;
    }

    /** 打开后处理器在等待时周期检查取消信号（用于验证协作式取消）。 */
    public LatchHandler cooperative() {
        this.checkCancellation = true;
        return this;
    }

    public int invocations() {
        return invocations.get();
    }

    public boolean awaitEntered(long millis) throws InterruptedException {
        return entered.await(millis, TimeUnit.MILLISECONDS);
    }

    public void releaseAll() {
        release.countDown();
    }

    @Override
    public void handle(String payload, TaskContext context) throws Exception {
        int n = invocations.incrementAndGet();
        entered.countDown();
        if (failFirstN > 0 && n <= failFirstN) {
            if (nonRetryable) {
                throw new NonRetryableTaskException("不可重试的模拟失败 #" + n);
            }
            throw new IllegalStateException("可重试的模拟失败 #" + n);
        }
        while (release.getCount() > 0) {
            if (release.await(20, TimeUnit.MILLISECONDS)) {
                break;
            }
            if (checkCancellation && context.isCancelled()) {
                throw new InterruptedException("测试处理器感知到取消信号");
            }
        }
    }
}
