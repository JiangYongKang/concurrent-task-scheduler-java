package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.quota;

import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.QuotaSnapshot;

/**
 * 配额管理：按调用方/任务组限制并发度与每秒执行量。
 * 所有判定与记账必须原子完成，绝不越限。
 */
public interface QuotaManager {

    /** 尝试为指定调用方占用一个"可立即启动"名额；成功返回 true。 */
    boolean tryAcquireRunning(String caller);

    /** 尝试排入队列（调用方队列 + 全局队列）。 */
    boolean tryAcquireQueued(String caller);

    /** 从队列名额转为运行名额（派发时调用）。 */
    boolean tryPromoteQueuedToRunning(String caller);

    /** 运行结束释放运行名额。 */
    void releaseRunning(String caller);

    /** 排队任务终止（拒绝入队失败/取消排队/重启后丢弃）释放队列名额。 */
    void releaseQueued(String caller);

    /** 重启后重建占用计数。 */
    void recover(int running, java.util.Map<String, Integer> runningByCaller,
                 int queued, java.util.Map<String, Integer> queuedByCaller);

    QuotaSnapshot globalSnapshot();

    QuotaSnapshot callerSnapshot(String caller);
}
