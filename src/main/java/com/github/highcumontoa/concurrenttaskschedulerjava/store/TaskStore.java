package com.github.highcumontoa.concurrenttaskschedulerjava.store;

import com.github.highcumontoa.concurrenttaskschedulerjava.model.TaskRecord;

import java.util.List;
import java.util.Optional;

/**
 * 任务持久化存储。
 *
 * <p>append 以追加 WAL（JSON Lines）方式持久化一次状态快照，返回后即视为已落盘。
 * 加载时按版本顺序回放，每个任务取最新快照，从而在进程重启后恢复一致状态，
 * 不丢任务、不回退状态。
 */
public interface TaskStore {

    /** 追加任务快照（插入或状态更新）。 */
    void append(TaskRecord record);

    /** 按提交标识查询。 */
    Optional<TaskRecord> find(String taskId);

    /** 全部任务（恢复与管理/测试使用）。 */
    List<TaskRecord> loadAll();
}
