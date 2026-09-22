package com.github.highcumontoa.concurrenttaskschedulerjava.tests;

import com.github.highcumontoa.concurrenttaskschedulerjava.config.SchedulerProperties;
import com.github.highcumontoa.concurrenttaskschedulerjava.handler.DefaultHandlerRegistry;
import com.github.highcumontoa.concurrenttaskschedulerjava.handler.SampleTaskHandler;
import com.github.highcumontoa.concurrenttaskschedulerjava.model.TaskStatus;
import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaManager;
import com.github.highcumontoa.concurrenttaskschedulerjava.service.TaskSchedulerService;
import com.github.highcumontoa.concurrenttaskschedulerjava.store.WalTaskStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** 最小冒烟测试：服务装配 + 单任务执行成功。 */
class SmokeTests {

    @Test
    void singleTaskSucceeds(@TempDir Path dir) {
        SchedulerProperties props = new SchedulerProperties();
        props.setWalFile(dir.resolve("smoke.wal").toString());
        props.setWalFsync(false);
        props.setWorkerThreads(2);
        WalTaskStore store = new WalTaskStore(props.getWalFile(), false);
        QuotaManager qm = new QuotaManager();
        DefaultHandlerRegistry registry = new DefaultHandlerRegistry();
        registry.register(new SampleTaskHandler());
        TaskSchedulerService service = new TaskSchedulerService(store, qm, registry, props);
        service.start();
        try {
            service.submit("t1", "alice", "default", "sample", "hello", null);
            await().untilAsserted(() ->
                    assertEquals(TaskStatus.SUCCEEDED, service.get("t1").orElseThrow().getStatus()));
            assertEquals("echo: hello", service.get("t1").orElseThrow().getResult());
        } finally {
            service.shutdown();
            store.close();
        }
    }
}
