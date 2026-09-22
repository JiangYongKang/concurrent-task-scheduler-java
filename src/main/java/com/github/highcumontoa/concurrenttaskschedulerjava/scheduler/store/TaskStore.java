package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.store;

import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.TaskRecord;

import java.util.List;

/**
 * 任务记录持久化接口。
 * 实现必须保证：同一 (caller, submitKey) 的并发 putIfAbsent 只有一个成功。
 */
public interface TaskStore {

    /**
     * 幂等插入。
     *
     * @return 若 submitKey 已存在，返回已有记录（本次插入被忽略）；否则写入并返回 null
     */
    TaskRecord putIfAbsent(TaskRecord record);

    /** 覆盖写入（状态推进），先落盘再返回。 */
    void save(TaskRecord record);

    TaskRecord findById(String taskId);

    TaskRecord findBySubmitKey(String caller, String submitKey);

    /** 启动恢复：加载全部任务记录。 */
    List<TaskRecord> loadAll();
}
