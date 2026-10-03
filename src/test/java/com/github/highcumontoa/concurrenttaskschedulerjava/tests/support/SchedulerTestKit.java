package com.github.highcumontoa.concurrenttaskschedulerjava.tests.support;

import com.github.highcumontoa.concurrenttaskschedulerjava.config.SchedulerProperties;
import com.github.highcumontoa.concurrenttaskschedulerjava.governance.GovernanceStore;
import com.github.highcumontoa.concurrenttaskschedulerjava.handler.DefaultHandlerRegistry;
import com.github.highcumontoa.concurrenttaskschedulerjava.handler.SampleTaskHandler;
import com.github.highcumontoa.concurrenttaskschedulerjava.handler.TaskHandler;
import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaLimits;
import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaManager;
import com.github.highcumontoa.concurrenttaskschedulerjava.service.TaskSchedulerService;
import com.github.highcumontoa.concurrenttaskschedulerjava.store.WalTaskStore;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/** 测试用服务构造工具：独立 WAL + 可定制配额；restart 用同一 WAL 与处理器工厂模拟进程重启。 */
public final class SchedulerTestKit implements AutoCloseable {

    public final SchedulerProperties props;
    public WalTaskStore store;
    public GovernanceStore governanceStore;
    public QuotaManager quotaManager;
    public DefaultHandlerRegistry registry;
    public TaskSchedulerService service;
    private final List<Supplier<TaskHandler>> handlerFactories = new ArrayList<>();
    private boolean closed;

    private SchedulerTestKit(SchedulerProperties props) {
        this.props = props;
    }

    public static Builder builder(Path walDir) {
        return new Builder(walDir);
    }

    private void boot(boolean fresh) {
        if (!fresh) {
            store = new WalTaskStore(props.getWalFile(), false);
        }
        governanceStore = new GovernanceStore(props.getGovernanceFile(), false);
        quotaManager = new QuotaManager();
        quotaManager.setDefaultLimits(new QuotaLimits(props.getDefaultMaxConcurrency(),
                props.getDefaultRateLimitPerSecond(), props.getDefaultMaxQueued()));
        registry = new DefaultHandlerRegistry();
        handlerFactories.forEach(f -> registry.register(f.get()));
        service = new TaskSchedulerService(store, quotaManager, registry, props, governanceStore);
        service.start();
    }

    /** 关闭当前实例并用同一 WAL 重建（模拟进程重启）。 */
    public SchedulerTestKit restart() {
        service.shutdown();
        store.close();
        governanceStore.close();
        SchedulerTestKit kit = new SchedulerTestKit(props);
        kit.handlerFactories.addAll(this.handlerFactories);
        kit.boot(false);
        return kit;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        service.shutdown();
        store.close();
        governanceStore.close();
    }

    public static final class Builder {
        private final Path walDir;
        private int workers = 4;
        private int maxConcurrency = 2;
        private int ratePerSecond = 0;
        private int maxQueued = 100;
        private long timeout = 30_000;
        private int maxRetries = 2;
        private long backoffBase = 50;
        private final List<Supplier<TaskHandler>> factories = new ArrayList<>();

        private Builder(Path walDir) {
            this.walDir = walDir;
        }

        public Builder workers(int v) { this.workers = v; return this; }
        public Builder concurrency(int v) { this.maxConcurrency = v; return this; }
        public Builder ratePerSecond(int v) { this.ratePerSecond = v; return this; }
        public Builder maxQueued(int v) { this.maxQueued = v; return this; }
        public Builder timeout(long v) { this.timeout = v; return this; }
        public Builder maxRetries(int v) { this.maxRetries = v; return this; }
        public Builder backoffBase(long v) { this.backoffBase = v; return this; }
        public Builder sampleHandler() { this.factories.add(SampleTaskHandler::new); return this; }
        public Builder handler(Supplier<TaskHandler> factory) {
            this.factories.add(factory);
            return this;
        }

        public SchedulerTestKit build() {
            SchedulerProperties props = new SchedulerProperties();
            props.setWalFile(walDir.resolve("scheduler.wal").toString());
            props.setGovernanceFile(walDir.resolve("governance.wal").toString());
            props.setWalFsync(false);
            props.setWorkerThreads(workers);
            props.setDefaultMaxConcurrency(maxConcurrency);
            props.setDefaultRateLimitPerSecond(ratePerSecond);
            props.setDefaultMaxQueued(maxQueued);
            props.setDefaultTimeoutMillis(timeout);
            props.setDefaultMaxRetries(maxRetries);
            props.setBackoffBaseMillis(backoffBase);
            props.setBackoffMultiplier(2.0);
            props.setBackoffCapMillis(5_000);
            props.setBackoffJitter(0.0);
            SchedulerTestKit kit = new SchedulerTestKit(props);
            kit.handlerFactories.addAll(factories);
            kit.store = new WalTaskStore(props.getWalFile(), false);
            kit.boot(true);
            return kit;
        }
    }
}
