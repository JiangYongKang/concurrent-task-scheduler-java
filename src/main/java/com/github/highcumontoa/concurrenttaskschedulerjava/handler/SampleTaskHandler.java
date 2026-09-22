package com.github.highcumontoa.concurrenttaskschedulerjava.handler;

import com.github.highcumontoa.concurrenttaskschedulerjava.retry.NonRetryableTaskException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 内置示例处理器（type = "sample"）。
 *
 * <p>payload 为简单文本约定（无需 JSON 解析）：
 * <ul>
 *   <li>"sleep:N"：睡眠 N 毫秒（用于验证并发、超时、取消，期间响应中断）；</li>
 *   <li>"fail:msg"：抛出可重试异常；</li>
 *   <li>"fatal:msg"：抛出 {@link NonRetryableTaskException}，立即失败不重试；</li>
 *   <li>其他：立即成功并原样回显。</li>
 * </ul>
 */
@Component
public class SampleTaskHandler implements TaskHandler {

    private static final Logger log = LoggerFactory.getLogger(SampleTaskHandler.class);

    @Override
    public String type() {
        return "sample";
    }

    @Override
    public String execute(String payload, TaskContext ctx) throws Exception {
        String p = payload == null ? "" : payload;
        if (p.startsWith("sleep:")) {
            long ms = Long.parseLong(p.substring("sleep:".length()));
            long deadline = System.currentTimeMillis() + ms;
            while (System.currentTimeMillis() < deadline) {
                if (ctx.cancelled()) {
                    throw new InterruptedException("task cancelled during sleep");
                }
                Thread.sleep(Math.min(50L, deadline - System.currentTimeMillis()));
            }
            log.info("sample sleep 完成 taskId={} attempt={}", ctx.taskId(), ctx.attempt());
            return "slept " + ms + "ms";
        }
        if (p.startsWith("fatal:")) {
            throw new NonRetryableTaskException(p.substring("fatal:".length()));
        }
        if (p.startsWith("fail:")) {
            throw new IllegalStateException(p.substring("fail:".length()));
        }
        return "echo: " + p;
    }
}
