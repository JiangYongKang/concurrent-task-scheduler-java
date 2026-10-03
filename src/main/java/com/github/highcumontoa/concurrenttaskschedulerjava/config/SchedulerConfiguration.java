package com.github.highcumontoa.concurrenttaskschedulerjava.config;

import com.github.highcumontoa.concurrenttaskschedulerjava.handler.DefaultHandlerRegistry;
import com.github.highcumontoa.concurrenttaskschedulerjava.handler.HandlerRegistry;
import com.github.highcumontoa.concurrenttaskschedulerjava.governance.FileGovernanceStore;
import com.github.highcumontoa.concurrenttaskschedulerjava.governance.GovernanceStore;
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

    /** 运行期治理事件日志（配额调整 / 暂停 / 恢复），独立于任务 WAL。 */
    @Bean
    public GovernanceStore governanceStore(SchedulerProperties props) {
        return new FileGovernanceStore(resolveGovernanceFile(props), props.isWalFsync());
    }

    /** 治理日志路径：未配置时取 WAL 同目录下 {@code <walName>.governance.jsonl}。 */
    static String resolveGovernanceFile(SchedulerProperties props) {
        String configured = props.getGovernanceFile();
        if (configured != null && !configured.isBlank()) {
            return configured;
        }
        java.nio.file.Path wal = java.nio.file.Paths.get(props.getWalFile());
        java.nio.file.Path parent = wal.toAbsolutePath().getParent();
        String fileName = (wal.getFileName() == null ? "task-scheduler.wal"
                : wal.getFileName().toString()) + ".governance.jsonl";
        return (parent == null ? java.nio.file.Paths.get(fileName)
                : parent.resolve(fileName)).toString();
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
    public TaskSchedulerService taskSchedulerService(TaskStore store,
                                                     GovernanceStore governanceStore,
                                                     QuotaManager quotaManager,
                                                     HandlerRegistry handlerRegistry,
                                                     SchedulerProperties props) {
        return new TaskSchedulerService(store, governanceStore, quotaManager, handlerRegistry,
                props);
    }
}
