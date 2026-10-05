package com.github.highcumontoa.concurrenttaskschedulerjava.web;

import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaStatus;
import com.github.highcumontoa.concurrenttaskschedulerjava.service.TaskSchedulerService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 运行期治理 REST 接口：配额调整、暂停/恢复派发、运行态查询。
 * 所有操作立即生效并持久化，进程重启后保持。
 *
 * <p>作用域：callerId 与 group 都给 = 精确维度；只给 callerId = 该调用方整体；
 * 只给 group = 该任务组整体；两者都不给 = 非法（缺少作用域信息，400）。
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

    /** 暂停派发：新任务照常排队，执行中任务自然跑完。 */
    @PostMapping("/pause")
    public QuotaStatus pause(@RequestBody PauseRequest request) {
        return service.pause(request.callerId(), request.group());
    }

    /** 恢复派发：排队任务按原公平顺序继续执行。 */
    @PostMapping("/resume")
    public QuotaStatus resume(@RequestBody PauseRequest request) {
        return service.resume(request.callerId(), request.group());
    }

    /**
     * 查询运行态：callerId 与 group 都给查精确维度；只给 callerId 查调用方整体；
     * 只给 group 查任务组整体；都不给则列出全部已知作用域。
     */
    @GetMapping("/status")
    public Object status(@RequestParam(required = false) String callerId,
                         @RequestParam(required = false) String group) {
        boolean hasCaller = callerId != null && !callerId.isBlank();
        boolean hasGroup = group != null && !group.isBlank();
        if (!hasCaller && !hasGroup) {
            return service.listGovernanceStatus();
        }
        return service.governanceStatus(hasCaller ? callerId : null,
                hasGroup ? group : null);
    }
}
