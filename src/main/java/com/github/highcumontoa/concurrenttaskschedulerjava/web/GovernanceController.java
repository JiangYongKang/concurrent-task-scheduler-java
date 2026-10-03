package com.github.highcumontoa.concurrenttaskschedulerjava.web;

import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaStatus;
import com.github.highcumontoa.concurrenttaskschedulerjava.service.TaskSchedulerService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 运行期治理 REST 接口：配额调整、暂停/恢复派发、运行态查询。
 * 所有操作立即生效并持久化，进程重启后保持。
 */
@RestController
@RequestMapping("/api/governance")
public class GovernanceController {

    private final TaskSchedulerService service;

    public GovernanceController(TaskSchedulerService service) {
        this.service = service;
    }

    /** 调整配额请求；三项均必填且 >= 0（0 表示不限）。 */
    public record AdjustQuotaRequest(String callerId, String group,
                                     Integer maxConcurrency, Integer rateLimitPerSecond,
                                     Integer maxQueued) {
    }

    /** 暂停/恢复请求。 */
    public record PauseRequest(String callerId, String group) {
    }

    /** 运行期调整配额：立即参与后续调度判定；非法值返回 400 且原值不变。 */
    @PutMapping("/quota")
    public QuotaStatus adjustQuota(@RequestBody AdjustQuotaRequest request) {
        return service.adjustQuota(request.callerId(), request.group(),
                request.maxConcurrency(), request.rateLimitPerSecond(), request.maxQueued());
    }

    /** 暂停某维度派发：新任务照常排队，执行中任务自然跑完。 */
    @PostMapping("/pause")
    public QuotaStatus pause(@RequestBody PauseRequest request) {
        return service.pause(request.callerId(), request.group());
    }

    /** 恢复某维度派发：排队任务按原公平顺序继续执行。 */
    @PostMapping("/resume")
    public QuotaStatus resume(@RequestBody PauseRequest request) {
        return service.resume(request.callerId(), request.group());
    }

    /** 查询运行态：带 callerId 查单个维度，不带则列出全部已知维度。 */
    @GetMapping("/status")
    public Object status(@RequestParam(required = false) String callerId,
                         @RequestParam(required = false) String group) {
        if (callerId == null || callerId.isBlank()) {
            List<QuotaStatus> all = service.listGovernanceStatus();
            return all;
        }
        return service.governanceStatus(callerId, group);
    }
}
