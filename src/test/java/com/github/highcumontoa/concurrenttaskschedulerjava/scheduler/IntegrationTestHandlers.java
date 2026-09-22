package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler;

import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler.TaskContext;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler.TaskHandler;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** 集成测试用处理器 Bean（位于组件扫描路径内）。 */
@TestConfiguration
public class IntegrationTestHandlers {

    public static final CountDownLatch ENTERED = new CountDownLatch(1);
    public static final CountDownLatch RELEASE = new CountDownLatch(1);

    @Bean
    public TaskHandler itBlockingHandler() {
        return new TaskHandler() {
            @Override
            public String type() {
                return "it-blocking";
            }

            @Override
            public void handle(String payload, TaskContext context) throws Exception {
                ENTERED.countDown();
                RELEASE.await(10, TimeUnit.SECONDS);
            }
        };
    }
}
