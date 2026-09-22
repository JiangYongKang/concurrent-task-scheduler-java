package com.github.highcumontoa.concurrenttaskschedulerjava.tests;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.file.Files;
import java.nio.file.Path;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * HTTP 层集成测试：使用独立测试配置（见 application-test.properties），
 * 验证 accepted / duplicate / rejected / 404 的响应约定稳定可区分。
 */
@SpringBootTest(properties = {
        "task.scheduler.wal-file=target/test-data/it.wal",
        "task.scheduler.wal-fsync=false",
        "task.scheduler.worker-threads=2",
        "task.scheduler.default-max-concurrency=1",
        "task.scheduler.default-rate-limit-per-second=10",
        "task.scheduler.default-max-queued=1",
        "task.scheduler.default-timeout-millis=5000",
})
@AutoConfigureMockMvc
class TaskControllerIntegrationTests {

    @Autowired
    private MockMvc mvc;

    @BeforeAll
    static void cleanWal() throws Exception {
        Files.deleteIfExists(Path.of("target/test-data/it.wal"));
    }

    @Test
    void submitDuplicateRejectAndUnknownTypeAreDistinguishable() throws Exception {
        // 1) 接受：立即成功的 sample 任务
        mvc.perform(post("/api/tasks")
                        .contentType("application/json")
                        .content("""
                                {"taskId":"it-1","callerId":"alice","group":"g","taskType":"sample","payload":"hi"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accepted").value(true))
                .andExpect(jsonPath("$.duplicate").value(false));

        // 2) 查询：等待成功后结果可见
        org.awaitility.Awaitility.await().atMost(5, java.util.concurrent.TimeUnit.SECONDS)
                .untilAsserted(() -> mvc.perform(get("/api/tasks/it-1"))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.status").value("SUCCEEDED"))
                        .andExpect(jsonPath("$.result").value("echo: hi")));

        // 3) 重复提交：200 + duplicate=true + 当前状态
        mvc.perform(post("/api/tasks")
                        .contentType("application/json")
                        .content("""
                                {"taskId":"it-1","callerId":"alice","group":"g","taskType":"sample","payload":"hi"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.duplicate").value(true))
                .andExpect(jsonPath("$.status").value("SUCCEEDED"));

        // 4) 未知类型：404 + UNKNOWN_TASK_TYPE
        mvc.perform(post("/api/tasks")
                        .contentType("application/json")
                        .content("""
                                {"taskId":"it-x","callerId":"alice","group":"g","taskType":"nope"}
                                """))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.rejectReason").value("UNKNOWN_TASK_TYPE"));

        // 5) 查询不存在：404 + TASK_NOT_FOUND
        mvc.perform(get("/api/tasks/missing")).andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("TASK_NOT_FOUND"));
    }

    @Test
    void quotaExhaustionReturns429() throws Exception {
        // maxConcurrency=1, maxQueued=1：一个睡眠任务占满并发，第二个排队，第三个必须 429
        mvc.perform(post("/api/tasks")
                        .contentType("application/json")
                        .content("""
                                {"taskId":"it-busy","callerId":"bob","group":"gq","taskType":"sample","payload":"sleep:3000"}
                                """))
                .andExpect(status().isOk()).andExpect(jsonPath("$.accepted").value(true));
        // 等待其进入 RUNNING
        Thread.sleep(150);
        mvc.perform(post("/api/tasks")
                        .contentType("application/json")
                        .content("""
                                {"taskId":"it-wait","callerId":"bob","group":"gq","taskType":"sample","payload":"sleep:0"}
                                """))
                .andExpect(status().isOk());
        mvc.perform(post("/api/tasks")
                        .contentType("application/json")
                        .content("""
                                {"taskId":"it-over","callerId":"bob","group":"gq","taskType":"sample","payload":"sleep:0"}
                                """))
                .andExpect(status().isTooManyRequests())
                .andExpect(jsonPath("$.accepted").value(false))
                .andExpect(jsonPath("$.rejectReason").value("QUOTA_EXHAUSTED"));
    }
}
