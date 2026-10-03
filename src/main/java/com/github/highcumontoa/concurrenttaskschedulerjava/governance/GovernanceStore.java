package com.github.highcumontoa.concurrenttaskschedulerjava.governance;

import java.util.List;

/**
 * 运行期治理事件持久化存储。
 *
 * <p>append 返回即视为治理变更已落盘；loadAll 按写入顺序返回全部事件，
 * 调度器启动时顺序回放，每个 scope 以最后一条事件为准。
 */
public interface GovernanceStore {

    /** 追加一条治理事件。 */
    void append(GovernanceEvent event);

    /** 按写入顺序加载全部治理事件（启动回放使用）。 */
    List<GovernanceEvent> loadAll();
}
