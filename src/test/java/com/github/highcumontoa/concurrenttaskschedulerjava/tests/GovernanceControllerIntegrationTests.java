package com.github.highcumontoa.concurrenttaskschedulerjava.tests;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * 运行期治理 HTTP 接口集成测试：
 * 配额调整立即生效、暂停/恢复状态可查、非法值 400 且原值不变、作用域参数缺失 400。
 */
@SpringBootTest(properties = {
        "task.scheduler.wal-file=target/test-data/gov-it.wal",
        "task.scheduler.governance-file=target/test-data/gov-it.governance.jsonl",
        "task.scheduler.wal-fsync=false",
        "task.scheduler.worker-threads=2",
        "task.scheduler.default-max-concurrency=2",
        "task.scheduler.default-rate-limit-per-second=0",
        "task.scheduler.default-max-queued=100",
})
@AutoConfigureMockMvc
class GovernanceControllerIntegrationTests {

    private MockMvc mvc;

    @BeforeAll
    static void clean() throws Exception {
        Files.deleteIfExists(Path.of("target/test-data/gov-it.wal"));
        Files.deleteIfExists(Path.of("target/test-data/gov-it.governance.jsonl"));
    }

    @org.springframework.beans.factory.annotation.Autowired
    public void setMvc(MockMvc mvc) {
        this.mvc = mvc;
    }

    @Test
    void adjustPauseResumeAndStatusOverHttp() throws Exception {
        // 初始状态
        mvc.perform(get("/api/governance/status").param("callerId", "mallory").param("group", "g"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.paused").value(false))
                .andExpect(jsonPath("$.limits.maxConcurrency").value(2));

        // 调整配额
        mvc.perform(post("/api/governance/quotas")
                        .param("callerId", "mallory").param("group", "g")
                        .contentType("application/json")
                        .content("""
                                {"maxConcurrency":1,"reason":"spike"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.paused").value(false))
                .andExpect(jsonPath("$.maxConcurrency").value(1))
                .andExpect(jsonPath("$.rateLimitPerSecond").value(0))
                .andExpect(jsonPath("$.maxQueued").value(100));

        mvc.perform(get("/api/governance/status").param("callerId", "mallory").param("group", "g"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.limits.maxConcurrency").value(1))
                .andExpect(jsonPath("$.lastChangeDescription").value(org.hamcrest.Matchers
                        .containsString("adjust-limits")));

        // 非法值：400，原值不变
        mvc.perform(post("/api/governance/quotas")
                        .param("callerId", "mallory").param("group", "g")
                        .contentType("application/json")
                        .content("""
                                {"maxConcurrency":-7}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
        mvc.perform(get("/api/governance/status").param("callerId", "mallory").param("group", "g"))
                .andExpect(jsonPath("$.limits.maxConcurrency").value(1));

        // 无作用域：400
        mvc.perform(post("/api/governance/pause"))
                .andExpect(status().isBadRequest());

        // 暂停 / 恢复
        mvc.perform(post("/api/governance/pause")
                        .param("callerId", "mallory").param("group", "g")
                        .param("reason", "freeze"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.paused").value(true))
                .andExpect(jsonPath("$.scopeKind").value("EXACT"));
        mvc.perform(get("/api/governance/status").param("callerId", "mallory").param("group", "g"))
                .andExpect(jsonPath("$.paused").value(true));

        mvc.perform(post("/api/governance/resume")
                        .param("callerId", "mallory").param("group", "g"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.paused").value(false));
        mvc.perform(get("/api/governance/status").param("callerId", "mallory").param("group", "g"))
                .andExpect(jsonPath("$.paused").value(false));
    }
}
