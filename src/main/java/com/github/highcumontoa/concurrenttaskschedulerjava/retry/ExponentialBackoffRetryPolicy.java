package com.github.highcumontoa.concurrenttaskschedulerjava.retry;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 指数退避重试策略。
 *
 * <p>退避公式：min(cap, base * multiplier^(retryIndex-1))，可叠加比例抖动：
 * delay = value * (1 + jitterFactor * random[-1,1])。
 * jitterFactor=0 时结果完全确定，便于测试与解释。
 *
 * <p>错误分类：
 * <ul>
 *   <li>{@link NonRetryableTaskException} 标记为不可重试，立即失败；</li>
 *   <li>{@link InterruptedException} 视为取消，立即终止为 CANCELLED；</li>
 *   <li>其余异常视为可重试（执行超时由调度器在 Future 层单独判定为 TIMEOUT，不进入此处）。</li>
 * </ul>
 */
public class ExponentialBackoffRetryPolicy implements RetryPolicy {

    private final int maxRetries;
    private final long baseMillis;
    private final double multiplier;
    private final long capMillis;
    private final double jitterFactor;

    public ExponentialBackoffRetryPolicy(int maxRetries, long baseMillis, double multiplier,
                                         long capMillis, double jitterFactor) {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be >= 0");
        }
        if (baseMillis < 0 || capMillis < 0) {
            throw new IllegalArgumentException("backoff millis must be >= 0");
        }
        if (jitterFactor < 0 || jitterFactor >= 1) {
            throw new IllegalArgumentException("jitterFactor must be in [0,1)");
        }
        this.maxRetries = maxRetries;
        this.baseMillis = baseMillis;
        this.multiplier = multiplier;
        this.capMillis = capMillis;
        this.jitterFactor = jitterFactor;
    }

    @Override
    public int maxRetries() {
        return maxRetries;
    }

    @Override
    public ErrorClassification classify(Throwable error) {
        if (error instanceof NonRetryableTaskException) {
            return ErrorClassification.NON_RETRYABLE;
        }
        if (error instanceof InterruptedException) {
            return ErrorClassification.CANCELLED;
        }
        return ErrorClassification.RETRYABLE;
    }

    @Override
    public long backoffMillis(int retryIndex) {
        if (retryIndex < 1) {
            throw new IllegalArgumentException("retryIndex must start from 1");
        }
        double raw = baseMillis * Math.pow(multiplier, retryIndex - 1);
        long value = (long) Math.min(capMillis, raw);
        if (jitterFactor > 0.0) {
            double ratio = 1.0 + jitterFactor * (2.0 * ThreadLocalRandom.current().nextDouble() - 1.0);
            value = (long) Math.max(0, value * ratio);
        }
        return value;
    }
}
