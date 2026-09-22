package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.web;

import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.store.TaskStoreException;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.web.TaskDtos.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 统一错误响应体 {@code {code, message}} 与稳定 HTTP 状态码：
 * 400 请求非法 / 未知任务类型，404 任务不存在，429 配额不足（提交接口语义化返回），
 * 500 持久化等内部错误。
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(ApiException.class)
    public ResponseEntity<ErrorResponse> handleApi(ApiException e) {
        String code = switch (e.getStatus()) {
            case 400 -> "BAD_REQUEST";
            case 404 -> "NOT_FOUND";
            case 429 -> "QUOTA_EXHAUSTED";
            default -> "API_ERROR";
        };
        return ResponseEntity.status(e.getStatus()).body(new ErrorResponse(code, e.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArg(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(new ErrorResponse("BAD_REQUEST", e.getMessage()));
    }

    @ExceptionHandler(TaskStoreException.class)
    public ResponseEntity<ErrorResponse> handleStore(TaskStoreException e) {
        log.error("持久化层错误", e);
        return ResponseEntity.status(500).body(new ErrorResponse("STORE_FAILURE", e.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleOther(Exception e) {
        log.error("未预期的服务端错误", e);
        return ResponseEntity.status(500).body(new ErrorResponse("INTERNAL_ERROR", e.getMessage()));
    }
}
