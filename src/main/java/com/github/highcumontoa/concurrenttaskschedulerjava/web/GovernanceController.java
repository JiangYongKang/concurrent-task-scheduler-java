package com.github.highcumontoa.concurrenttaskschedulerjava.web;

import com.github.highcumontoa.concurrenttaskschedulerjava.quota.QuotaStatus;
import com.github.highcumontoa.concurrenttaskschedulerjava.service.TaskSchedulerService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 运行期治理 REST 接口：配额调整、暂停/恢复派发、运行态查询。
 * 所有操作立即生效并持久化，进程重启后保持。
 *
 * <p>作用域由 callerId / group 的给定组合决定：
 * 都给定为精确维度；只给 callerId 作用于该调用方全部任务组；
 * 只给 group 作用于该任务组全部调用方；两者都缺失返回 400。
 */
@RestController
@RequestMapping("/api/governance")
public class GovernanceController {

    private final TaskSchedulerService service;

    public GovernanceController(TaskSchedulerService service) {
        this.service = service;
    }

    /**
     * 调整配额请求。callerId / group 至少给一个（只给 callerId 为调用方级，
     * 只给 group 为任务组级）；三项配额均必填且 &gt;= 0（0 表示不限）。
     */
    public record AdjustQuotaRequest(String callerId, String group,
                                     Integer maxConcurrency, Integer rateLimitPerSecond,
                                     Integer maxQueued) {
    }

    /** 暂停/恢复请求；callerId / group 至少给一个。 */
    public record PauseRequest(String callerId, String group) {
    }

    /** 运行期调整配额：立即参与后续调度判定；非法值返回 400 且原值不变。 */
    @PutMapping("/quota")
    public QuotaStatus adjustQuota(@RequestBody AdjustQuotaRequest request) {
        return service.adjustQuota(request.callerId(), request.group(),
                request.maxConcurrency(), request.rateLimitPerSecond(), request.maxQueued());
    }

    /** 暂停某作用域派发：新任务照常排队，执行中任务自然跑完。 */
    @PostMapping("/pause")
    public QuotaStatus pause(@RequestBody PauseRequest request) {
        return service.pause(request.callerId(), request.group());
    }

    /** 恢复某作用域派发：排队任务按原公平顺序继续执行。 */
    @PostMapping("/resume")
    public QuotaStatus resume(@RequestBody PauseRequest request) {
        return service.resume(request.callerId(), request.group());
    }

    /**
     * 查询运行态：callerId+group 查精确维度；只带 callerId 查该调用方整体；
     * 只带 group 查该任务组整体；都不带列出全部已知维度与作用域。
     */
    @GetMapping("/status")
    public Object status(@RequestParam(required = false) String callerId,
                         @RequestParam(required = false) String group) {
        boolean hasCaller = callerId != null && !callerId.isBlank();
        boolean hasGroup = group != null && !group.isBlank();
        if (!hasCaller && !hasGroup) {
            List<QuotaStatus> all = service.listGovernanceStatus();
            return all;
        }
        return service.governanceStatus(callerId, group);
    }
}
