package com.github.highcumontoa.concurrenttaskschedulerjava.config;

import com.github.highcumontoa.concurrenttaskschedulerjava.handler.DefaultHandlerRegistry;
import com.github.highcumontoa.concurrenttaskschedulerjava.handler.HandlerRegistry;
import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaKey;
import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaLimits;
import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaManager;
import com.github.highcumontoa.concurrenttaskschedulerjava.service.TaskSchedulerService;
import com.github.highcumontoa.concurrenttaskschedulerjava.store.TaskStore;
import com.github.highcumontoa.concurrenttaskschedulerjava.store.WalTaskStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 调度器装配：存储、配额、注册表、服务均为本地组件，无外部中间件依赖。 */
@Configuration
@EnableConfigurationProperties(SchedulerProperties.class)
public class SchedulerConfiguration {

    @Bean
    public TaskStore taskStore(SchedulerProperties props) {
        return new WalTaskStore(props.getWalFile(), props.isWalFsync());
    }

    @Bean
    public QuotaManager quotaManager(SchedulerProperties props) {
        QuotaManager manager = new QuotaManager();
        manager.setDefaultLimits(new QuotaLimits(props.getDefaultMaxConcurrency(),
                props.getDefaultRateLimitPerSecond(), props.getDefaultMaxQueued()));
        props.getQuotas().forEach((key, override) -> {
            QuotaKey qk = parseQuotaKey(key);
            manager.setLimits(qk, new QuotaLimits(
                    override.getMaxConcurrency() != null
                            ? override.getMaxConcurrency() : props.getDefaultMaxConcurrency(),
                    override.getRateLimitPerSecond() != null
                            ? override.getRateLimitPerSecond() : props.getDefaultRateLimitPerSecond(),
                    override.getMaxQueued() != null
                            ? override.getMaxQueued() : props.getDefaultMaxQueued()));
        });
        return manager;
    }

    /** 支持 "caller"（作用于该 caller 的 default 组）与 "caller:group" 两种键。 */
    private static QuotaKey parseQuotaKey(String raw) {
        int idx = raw.indexOf(':');
        if (idx < 0) {
            return QuotaKey.of(raw.trim(), "default");
        }
        return QuotaKey.of(raw.substring(0, idx).trim(), raw.substring(idx + 1).trim());
    }

    @Bean
    public HandlerRegistry handlerRegistry(java.util.List<com.github.highcumontoa.concurrenttaskschedulerjava.handler.TaskHandler> beans) {
        DefaultHandlerRegistry registry = new DefaultHandlerRegistry();
        beans.forEach(registry::register);
        return registry;
    }

    @Bean
    public TaskSchedulerService taskSchedulerService(TaskStore store, QuotaManager quotaManager,
                                                     HandlerRegistry handlerRegistry,
                                                     SchedulerProperties props) {
        return new TaskSchedulerService(store, quotaManager, handlerRegistry, props);
    }
}
