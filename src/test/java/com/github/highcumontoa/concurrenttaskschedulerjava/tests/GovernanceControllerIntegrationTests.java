package com.github.highcumontoa.concurrenttaskschedulerjava.tests;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 运行期治理 HTTP 接口测试：配额调整、非法值 400、暂停/恢复、运行态查询。
 */
@SpringBootTest(properties = {
        "task.scheduler.wal-file=target/test-data/it-governance.wal",
        "task.scheduler.governance-file=target/test-data/it-governance.gov.wal",
        "task.scheduler.wal-fsync=false",
        "task.scheduler.worker-threads=2",
        "task.scheduler.default-max-concurrency=2",
        "task.scheduler.default-rate-limit-per-second=0",
        "task.scheduler.default-max-queued=100",
})
@AutoConfigureMockMvc
class GovernanceControllerIntegrationTests {

    @Autowired
    private MockMvc mvc;

    @BeforeAll
    static void cleanWal() throws Exception {
        Files.deleteIfExists(Path.of("target/test-data/it-governance.wal"));
        Files.deleteIfExists(Path.of("target/test-data/it-governance.gov.wal"));
    }

    @Test
    void adjustPauseResumeAndStatusOverHttp() throws Exception {
        // 1) 合法调整：200 + 生效值与操作元信息
        mvc.perform(put("/api/governance/quota")
                        .contentType("application/json")
                        .content("""
                                {"callerId":"erin","group":"g","maxConcurrency":3,"rateLimitPerSecond":0,"maxQueued":20}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.maxConcurrency").value(3))
                .andExpect(jsonPath("$.maxQueued").value(20))
                .andExpect(jsonPath("$.paused").value(false))
                .andExpect(jsonPath("$.lastOperation").value("ADJUST_QUOTA"));

        // 2) 非法调整（负值）：400，且生效值不变
        mvc.perform(put("/api/governance/quota")
                        .contentType("application/json")
                        .content("""
                                {"callerId":"erin","group":"g","maxConcurrency":-1,"rateLimitPerSecond":0,"maxQueued":20}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
        mvc.perform(get("/api/governance/status")
                        .param("callerId", "erin").param("group", "g"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.maxConcurrency").value(3));

        // 3) 缺项：400
        mvc.perform(put("/api/governance/quota")
                        .contentType("application/json")
                        .content("""
                                {"callerId":"erin","group":"g","maxConcurrency":2}
                                """))
                .andExpect(status().isBadRequest());

        // 4) 暂停 / 恢复
        mvc.perform(post("/api/governance/pause")
                        .contentType("application/json")
                        .content("""
                                {"callerId":"erin","group":"g"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.paused").value(true))
                .andExpect(jsonPath("$.lastOperation").value("PAUSE"));
        mvc.perform(post("/api/governance/resume")
                        .contentType("application/json")
                        .content("""
                                {"callerId":"erin","group":"g"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.paused").value(false))
                .andExpect(jsonPath("$.lastOperation").value("RESUME"));

        // 5) 列表查询包含该维度
        mvc.perform(get("/api/governance/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[?(@.callerId=='erin')].group").value("g"));
    }
}
