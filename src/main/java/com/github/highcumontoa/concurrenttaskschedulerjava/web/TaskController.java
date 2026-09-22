package com.github.highcumontoa.concurrenttaskschedulerjava.web;

import com.github.highcumontoa.concurrenttaskschedulerjava.model.TaskException;
import com.github.highcumontoa.concurrenttaskschedulerjava.model.TaskRecord;
import com.github.highcumontoa.concurrenttaskschedulerjava.service.SubmitResult;
import com.github.highcumontoa.concurrenttaskschedulerjava.service.TaskSchedulerService;
import com.github.highcumontoa.concurrenttaskschedulerjava.web.dto.SubmitTaskRequest;
import com.github.highcumontoa.concurrenttaskschedulerjava.web.dto.SubmitTaskResponse;
import com.github.highcumontoa.concurrenttaskschedulerjava.web.dto.TaskView;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/** 任务提交/查询/取消 REST 接口。 */
@RestController
@RequestMapping("/api/tasks")
public class TaskController {

    private final TaskSchedulerService service;

    public TaskController(TaskSchedulerService service) {
        this.service = service;
    }

    /**
     * 提交任务。
     * 200：接受（排队/执行中）或重复提交（duplicate=true）；
     * 429：配额耗尽被拒绝；400：参数非法；404：未知任务类型。
     */
    @PostMapping
    public ResponseEntity<SubmitTaskResponse> submit(@RequestBody SubmitTaskRequest request) {
        SubmitResult result = service.submit(request.getTaskId(), request.getCallerId(),
                request.getGroup(), request.getTaskType(), request.getPayload(),
                request.getTimeoutMillis());
        if (result.accepted() || result.duplicate()) {
            SubmitTaskResponse body = new SubmitTaskResponse(result.accepted(),
                    result.duplicate(), result.taskId(), result.status(),
                    result.rejectReason(), result.message());
            return ResponseEntity.ok(body);
        }
        HttpStatus status = switch (result.rejectReason()) {
            case UNKNOWN_TASK_TYPE -> HttpStatus.NOT_FOUND;
            case INVALID_REQUEST -> HttpStatus.BAD_REQUEST;
            case QUOTA_EXHAUSTED, QUEUE_FULL -> HttpStatus.TOO_MANY_REQUESTS;
        };
        return ResponseEntity.status(status).body(new SubmitTaskResponse(false, false,
                result.taskId(), result.status(), result.rejectReason(), result.message()));
    }

    @GetMapping("/{taskId}")
    public TaskView get(@PathVariable String taskId) {
        TaskRecord r = service.get(taskId)
                .orElseThrow(() -> new TaskException("TASK_NOT_FOUND", "task not found: " + taskId,
                        org.springframework.http.HttpStatus.NOT_FOUND));
        return TaskView.from(r);
    }

    @PostMapping("/{taskId}/cancel")
    public Map<String, Object> cancel(@PathVariable String taskId,
                                      @RequestParam(required = false) String reason) {
        boolean changed = service.cancel(taskId, reason);
        return Map.of("taskId", taskId, "cancelled", changed);
    }

    @GetMapping
    public List<TaskView> list() {
        return service.list().stream().map(TaskView::from).toList();
    }
}
