package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler.DefaultTaskHandlerRegistry;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler.TaskHandler;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.quota.InMemoryQuotaManager;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.quota.QuotaManager;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.service.DefaultTaskSchedulerService;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.service.TaskSchedulerService;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.store.FileTaskStore;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.store.TaskStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Paths;
import java.time.Clock;
import java.util.List;

/** 装配调度器各组件 Bean，并在容器启停时驱动调度器生命周期。 */
@Configuration
@EnableConfigurationProperties(SchedulerProperties.class)
public class SchedulerConfig {

    @Bean
    public Clock schedulerClock() {
        return Clock.systemUTC();
    }

    @Bean
    public ObjectMapper schedulerObjectMapper() {
        return new ObjectMapper().findAndRegisterModules();
    }

    @Bean
    public TaskStore taskStore(SchedulerProperties props, ObjectMapper schedulerObjectMapper) {
        return new FileTaskStore(Paths.get(props.getDataDir()), schedulerObjectMapper);
    }

    @Bean
    public QuotaManager quotaManager(SchedulerProperties props) {
        return new InMemoryQuotaManager(props);
    }

    @Bean
    public DefaultTaskHandlerRegistry taskHandlerRegistry(List<TaskHandler> handlers) {
        return new DefaultTaskHandlerRegistry(handlers);
    }

    @Bean(destroyMethod = "stop")
    public TaskSchedulerService taskSchedulerService(TaskStore taskStore,
                                                     QuotaManager quotaManager,
                                                     DefaultTaskHandlerRegistry registry,
                                                     SchedulerProperties props,
                                                     Clock schedulerClock) {
        validate(props);
        DefaultTaskSchedulerService service = new DefaultTaskSchedulerService(
                taskStore, quotaManager, registry, props, schedulerClock);
        service.start();
        return service;
    }

    /** fail-fast：配置在启动时即被拒绝，避免运行中出现诡异的配额/线程池行为。 */
    private void validate(SchedulerProperties p) {
        requirePositive(p.getGlobalMaxConcurrency(), "scheduler.global-max-concurrency");
        requirePositive(p.getGlobalMaxQueued(), "scheduler.global-max-queued");
        requirePositive(p.getGlobalRateLimitPerSecond(), "scheduler.global-rate-limit-per-second");
        requirePositive(p.getDefaultMaxConcurrency(), "scheduler.default-max-concurrency");
        requireNonNegative(p.getDefaultMaxQueued(), "scheduler.default-max-queued");
        requirePositive(p.getDefaultRateLimitPerSecond(), "scheduler.default-rate-limit-per-second");
        requirePositive(p.getDefaultMaxAttempts(), "scheduler.default-max-attempts");
        requireNonNegative(p.getAttemptTimeoutMillis(), "scheduler.attempt-timeout-millis");
        p.getCallers().forEach((name, c) -> {
            if (c.getMaxConcurrency() != null) {
                requirePositive(c.getMaxConcurrency(), "scheduler.callers." + name + ".max-concurrency");
            }
            if (c.getMaxQueued() != null) {
                requireNonNegative(c.getMaxQueued(), "scheduler.callers." + name + ".max-queued");
            }
            if (c.getRateLimitPerSecond() != null) {
                requirePositive(c.getRateLimitPerSecond(),
                        "scheduler.callers." + name + ".rate-limit-per-second");
            }
            if (c.getMaxAttempts() != null) {
                requirePositive(c.getMaxAttempts(), "scheduler.callers." + name + ".max-attempts");
            }
        });
    }

    private void requirePositive(int value, String key) {
        if (value <= 0) {
            throw new IllegalStateException("配置项 " + key + " 必须为正整数，当前值: " + value);
        }
    }

    private void requireNonNegative(long value, String key) {
        if (value < 0) {
            throw new IllegalStateException("配置项 " + key + " 不能为负数，当前值: " + value);
        }
    }

    private void requireNonNegative(int value, String key) {
        if (value < 0) {
            throw new IllegalStateException("配置项 " + key + " 不能为负数，当前值: " + value);
        }
    }
}
