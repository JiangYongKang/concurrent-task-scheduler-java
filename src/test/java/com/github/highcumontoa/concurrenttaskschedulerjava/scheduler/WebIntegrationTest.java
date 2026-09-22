package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler;

import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler.TaskContext;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.handler.TaskHandler;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 端到端：经真实 Spring 上下文 + MockMvc 验证 HTTP 语义、配额拒绝 429、
 * 重复提交返回同一任务、排队取消与配额查询。
 */
@SpringBootTest
@AutoConfigureMockMvc
@Import(IntegrationTestHandlers.class)
class WebIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(WebIntegrationTest.class);
    private static Path dataDir;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) throws Exception {
        dataDir = Files.createTempDirectory("scheduler-it-");
        registry.add("scheduler.data-dir", () -> dataDir.toString());
        registry.add("scheduler.global-max-concurrency", () -> 2);
        registry.add("scheduler.global-max-queued", () -> 4);
        registry.add("scheduler.global-rate-limit-per-second", () -> 1000);
        registry.add("scheduler.default-max-concurrency", () -> 1);
        registry.add("scheduler.default-max-queued", () -> 1);
        registry.add("scheduler.default-rate-limit-per-second", () -> 1000);
        registry.add("scheduler.attempt-timeout-millis", () -> 500);
        registry.add("scheduler.backoff-base-millis", () -> 10);
    }

    @Autowired
    private MockMvc mvc;

    @BeforeAll
    static void banner() {
        log.info("[集成测试] 启动全链路测试, dataDir={}", dataDir);
    }

    @Test
    void submitQueueRejectDedupCancelAndQuotaEndToEnd() throws Exception {
        // 1) 首个任务立即 START 并占满 team-it 并发
        MvcResult first = mvc.perform(post("/api/tasks")
                        .contentType("application/json")
                        .content("""
                                {"caller":"team-it","submitKey":"it-1","taskType":"it-blocking","payload":"a"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.verdict").value("START"))
                .andExpect(jsonPath("$.duplicate").value(false))
                .andExpect(jsonPath("$.status").value("RUNNING"))
                .andReturn();
        String firstId = com.jayway.jsonpath.JsonPath.read(first.getResponse().getContentAsString(), "$.taskId");
        org.assertj.core.api.Assertions.assertThat(IntegrationTestHandlers.ENTERED.await(3, TimeUnit.SECONDS)).isTrue();
        log.info("[集成测试] 首任务已运行 taskId={}", firstId);

        // 2) 第二个任务排队（调用方队列容量 1）
        mvc.perform(post("/api/tasks")
                        .contentType("application/json")
                        .content("""
                                {"caller":"team-it","submitKey":"it-2","taskType":"it-blocking","payload":"b"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.verdict").value("ENQUEUE"));

        // 3) 第三个任务：并发满 + 队列满 -> 429 REJECT
        mvc.perform(post("/api/tasks")
                        .contentType("application/json")
                        .content("""
                                {"caller":"team-it","submitKey":"it-3","taskType":"it-blocking","payload":"c"}
                                """))
                .andExpect(status().is(429))
                .andExpect(jsonPath("$.verdict").value("REJECT"))
                .andExpect(jsonPath("$.status").value("REJECTED"))
                .andExpect(jsonPath("$.rejectReason").value("CALLER_QUEUE_FULL"));
        log.info("[集成测试] 第三个任务被确定性拒绝 429");

        // 4) 重复提交 it-2：返回同一排队任务，duplicate=true
        MvcResult dup = mvc.perform(post("/api/tasks")
                        .contentType("application/json")
                        .content("""
                                {"caller":"team-it","submitKey":"it-2","taskType":"it-blocking","payload":"b"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.duplicate").value(true))
                .andExpect(jsonPath("$.verdict").value("ENQUEUE"))
                .andReturn();
        String dupId = com.jayway.jsonpath.JsonPath.read(dup.getResponse().getContentAsString(), "$.taskId");

        // 5) 取消排队任务 -> CANCELLED
        mvc.perform(post("/api/tasks/" + dupId + "/cancel"))
                .andExpect(jsonPath("$.status").value("CANCELLED"))
                .andExpect(jsonPath("$.failure.code").value("CANCELLED"));
        mvc.perform(get("/api/tasks/" + dupId))
                .andExpect(jsonPath("$.status").value("CANCELLED"));
        log.info("[集成测试] 排队任务取消成功 taskId={}", dupId);

        // 6) 配额视图
        mvc.perform(get("/api/quota").param("caller", "team-it"))
                .andExpect(jsonPath("$.scope").value("team-it"))
                .andExpect(jsonPath("$.running").value(1))
                .andExpect(jsonPath("$.queued").value(0));

        // 7) 参数错误 / 未知类型
        mvc.perform(post("/api/tasks")
                        .contentType("application/json")
                        .content("""
                                {"caller":"team-it","taskType":"not-exists"}
                                """))
                .andExpect(status().is(400))
                .andExpect(jsonPath("$.code").value("BAD_REQUEST"));

        // 8) 查询不存在任务 404
        mvc.perform(get("/api/tasks/nonexistent-id"))
                .andExpect(status().is(404))
                .andExpect(jsonPath("$.code").value("NOT_FOUND"));

        // 9) 放行首任务并确认完成
        IntegrationTestHandlers.RELEASE.countDown();
        org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(5)).untilAsserted(() ->
                mvc.perform(get("/api/tasks/" + firstId))
                        .andExpect(jsonPath("$.status").value("COMPLETED")));
        log.info("[集成测试] 首任务完成");
    }

    @Test
    void missingCallerReturns400() throws Exception {
        mvc.perform(post("/api/tasks")
                        .contentType("application/json")
                        .content("""
                                {"taskType":"it-blocking"}
                                """))
                .andExpect(status().is(400));
    }
}
