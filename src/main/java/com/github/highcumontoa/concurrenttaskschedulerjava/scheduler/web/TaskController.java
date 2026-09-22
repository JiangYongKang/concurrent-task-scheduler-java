package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.web;

import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.QuotaVerdict;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.SubmitResult;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.TaskRecord;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.service.TaskSchedulerService;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.web.TaskDtos.QuotaResponse;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.web.TaskDtos.SubmitRequest;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.web.TaskDtos.SubmitResponse;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.web.TaskDtos.TaskResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 任务调度 REST 接口：
 * <ul>
 *   <li>POST /api/tasks                       提交（同 submitKey 重复提交返回原任务，duplicate=true）</li>
 *   <li>GET  /api/tasks/{id}                  按任务 ID 查询</li>
 *   <li>GET  /api/tasks/by-key/{caller}/{key} 按提交幂等键查询</li>
 *   <li>POST /api/tasks/{id}/cancel           取消（排队任务立即 CANCELLED；运行中任务协作式停止）</li>
 *   <li>GET  /api/quota?caller=xxx            配额视图（不传 caller 返回全局）</li>
 * </ul>
 */
@RestController
@RequestMapping("/api")
public class TaskController {

    private final TaskSchedulerService service;

    public TaskController(TaskSchedulerService service) {
        this.service = service;
    }

    @PostMapping("/tasks")
    public ResponseEntity<SubmitResponse> submit(@RequestBody SubmitRequest request) {
        if (request == null || request.getTaskType() == null || request.getTaskType().isBlank()) {
            throw new ApiException(400, "taskType 不能为空");
        }
        if (request.getCaller() == null || request.getCaller().isBlank()) {
            throw new ApiException(400, "caller 不能为空");
        }
        SubmitResult result;
        try {
            result = service.submit(request.getCaller(), request.getSubmitKey(),
                    request.getTaskType(), request.getPayload());
        } catch (IllegalArgumentException e) {
            throw new ApiException(400, e.getMessage());
        }
        SubmitResponse body = SubmitResponse.from(result);
        if (result.getVerdict() == QuotaVerdict.REJECT) {
            return ResponseEntity.status(429).body(body);
        }
        return ResponseEntity.ok(body);
    }

    @GetMapping("/tasks/{id}")
    public TaskResponse get(@PathVariable String id) {
        TaskRecord r = service.get(id);
        if (r == null) {
            throw new ApiException(404, "任务不存在: " + id);
        }
        return TaskResponse.from(r);
    }

    @GetMapping("/tasks/by-key/{caller}/{submitKey}")
    public TaskResponse getByKey(@PathVariable String caller, @PathVariable String submitKey) {
        TaskRecord r = service.getBySubmitKey(caller, submitKey);
        if (r == null) {
            throw new ApiException(404, "任务不存在: caller=" + caller + ", submitKey=" + submitKey);
        }
        return TaskResponse.from(r);
    }

    @PostMapping("/tasks/{id}/cancel")
    public TaskResponse cancel(@PathVariable String id) {
        TaskRecord r = service.cancel(id);
        if (r == null) {
            throw new ApiException(404, "任务不存在: " + id);
        }
        return TaskResponse.from(r);
    }

    @GetMapping("/quota")
    public QuotaResponse quota(@RequestParam(required = false) String caller) {
        if (caller == null || caller.isBlank()) {
            return QuotaResponse.from("GLOBAL", service.quota(null));
        }
        return QuotaResponse.from(caller, service.quota(caller));
    }
}
