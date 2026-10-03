package com.github.highcumontoa.concurrenttaskschedulerjava.web;

import com.github.highcumontoa.concurrenttaskschedulerjava.governance.GovernanceChangeResult;
import com.github.highcumontoa.concurrenttaskschedulerjava.governance.GovernanceStatus;
import com.github.highcumontoa.concurrenttaskschedulerjava.service.TaskSchedulerService;
import com.github.highcumontoa.concurrenttaskschedulerjava.web.dto.GovernanceChangeResponse;
import com.github.highcumontoa.concurrenttaskschedulerjava.web.dto.QuotaAdjustmentRequest;
import org.springframework.web.bind.annotation.*;

/**
 * 运行期治理 REST 接口：按调用方/任务组调整配额、暂停/恢复派发、查询运行态。
 *
 * <p>作用域由 callerId / group 查询参数决定：两者都给=精确(caller,group)，
 * 只给 callerId=该调用方全部组，只给 group=该任务组全部调用方。
 * 非法参数（二者皆空、负数配额）返回 400，原有有效值不变。
 */
@RestController
@RequestMapping("/api/governance")
public class GovernanceController {

    private final TaskSchedulerService service;

    public GovernanceController(TaskSchedulerService service) {
        this.service = service;
    }

    /**
     * 运行期调整配额（PATCH 语义，立即生效）。
     * 三项为 null 表示不改；0 表示不限；负数返回 400 且原值不变。
     */
    @PostMapping("/quotas")
    public GovernanceChangeResponse adjustQuota(@RequestBody QuotaAdjustmentRequest request,
                                                @RequestParam(required = false) String callerId,
                                                @RequestParam(required = false) String group) {
        GovernanceChangeResult r = service.adjustQuota(callerId, group,
                request.getMaxConcurrency(), request.getRateLimitPerSecond(),
                request.getMaxQueued(), request.getReason());
        return GovernanceChangeResponse.from(r);
    }

    /** 暂停派发：新任务照常受理排队，已在执行的任务自然跑完。 */
    @PostMapping("/pause")
    public GovernanceChangeResponse pause(@RequestParam(required = false) String callerId,
                                          @RequestParam(required = false) String group,
                                          @RequestParam(required = false) String reason) {
        return GovernanceChangeResponse.from(service.pauseDispatch(callerId, group, reason));
    }

    /** 恢复派发：排队任务按原公平顺序继续执行。 */
    @PostMapping("/resume")
    public GovernanceChangeResponse resume(@RequestParam(required = false) String callerId,
                                           @RequestParam(required = false) String group,
                                           @RequestParam(required = false) String reason) {
        return GovernanceChangeResponse.from(service.resumeDispatch(callerId, group, reason));
    }

    /** 查询某精确维度（caller + group）的当前运行态。 */
    @GetMapping("/status")
    public GovernanceStatus status(@RequestParam String callerId,
                                   @RequestParam(defaultValue = "default") String group) {
        return service.governanceStatus(callerId, group);
    }
}
