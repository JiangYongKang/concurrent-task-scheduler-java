package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.demo;

import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler.NonRetryableTaskException;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler.TaskContext;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler.TaskHandler;
import org.springframework.stereotype.Component;

/**
 * 演示处理器：
 * payload=fatal 抛不可重试异常；fail 抛可重试异常；block 周期性等待并响应取消；其余立即成功。
 */
@Component
public class DemoHandler implements TaskHandler {

    @Override
    public String type() {
        return "demo";
    }

    @Override
    public void handle(String payload, TaskContext context) throws Exception {
        if ("fatal".equals(payload)) {
            throw new NonRetryableTaskException("演示不可重试错误");
        }
        if ("fail".equals(payload)) {
            throw new IllegalStateException("演示可重试失败");
        }
        if ("block".equals(payload)) {
            for (int i = 0; i < 600; i++) {
                Thread.sleep(50);
                if (context.isCancelled()) {
                    throw new InterruptedException("演示处理器收到取消信号");
                }
            }
        }
    }
}
