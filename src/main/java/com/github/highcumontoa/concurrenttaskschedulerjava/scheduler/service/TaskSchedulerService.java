package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.service;

import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.QuotaSnapshot;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.SubmitResult;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.TaskRecord;

/** 异步任务调度服务入口。 */
public interface TaskSchedulerService {

    SubmitResult submit(String caller, String submitKey, String taskType, String payload);

    TaskRecord get(String taskId);

    TaskRecord getBySubmitKey(String caller, String submitKey);

    TaskRecord cancel(String taskId);

    QuotaSnapshot quota(String caller);

    void start();

    void stop();
}
