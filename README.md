# 配额感知的本地异步任务调度器

在 Spring Boot（Java 21）工程内提供一套**纯本地**的配额感知异步任务调度能力：
多调用方并发提交批处理任务时，执行结果、资源占用与失败原因确定且可解释。
**不依赖任何外部消息队列、缓存、数据库或调度服务**——状态持久化只使用本地 JSON Lines WAL 文件。

## 1. 能力概览

- 异步任务的提交、排队与执行；按 `callerId + group`（调用方/任务组）两个维度限制
  **并发度**、**单位时间启动量**与**排队长度**。
- 配额不足时任务**排队**；排队也满时**拒绝**。排队 / 拒绝 / 超时 / 取消是四种可区分、稳定的结果。
- **运行期治理（无需重启）**：可按调用方或任务组在线调整三项配额、临时暂停/恢复派发、随时查询运行态；
  调整与暂停立即参与后续调度判定，且与配额本身一样**跨进程重启保持**。
- 提交幂等：相同 `taskId` 的重复提交（含高并发下的同时提交）只接受一次、只执行一次、只占一份配额。
- 状态持久化到本地 WAL；进程重启后终态不回退、排队任务不丢失、执行中崩溃的任务重新排队、配额不泄漏。
- 失败任务支持**指数退避重试**与终止策略；不可重试错误、取消、超时立即终止，绝不无限重试或重复执行。

## 2. 任务状态机

```
                 ┌──────────────────────── 重试退避到期 ───────────────────────┐
                 ▼                                                              │
QUEUED ──获得配额──▶ RUNNING ──成功──────────────────────────────────────▶ SUCCEEDED(终态)
  │                   │  │                                                    │
  │排队中取消          │  │可重试失败                                            │
  ▼                   │  ▼                                                    │
CANCELLED(终态)       │ PENDING_RETRY ──退避中取消──▶ CANCELLED(终态)            │
                      │  │                                                    │
                      │  │不可重试错误 / 执行超时 / 重试耗尽                      │
                      │  ▼                                                    │
                      └──────────────────────────────────────────────────▶ FAILED(终态)
                      │ 执行中取消（中断）──────────────────────────────▶ CANCELLED(终态)
```

| 状态 | 含义 | 是否占配额 |
| --- | --- | --- |
| `QUEUED` | 已持久化，等待并发/速率配额 | 占 1 个排队名额 |
| `RUNNING` | 已获得并发槽位，正在执行 | 占 1 个并发槽位 + 1 个启动速率额度 |
| `PENDING_RETRY` | 一次执行失败，等待指数退避 | 占 1 个排队名额（并发槽位已释放） |
| `SUCCEEDED` | 成功完成（终态） | 不占 |
| `FAILED` | 最终失败：不可重试 / 超时 / 重试耗尽（终态） | 不占 |
| `CANCELLED` | 排队中或执行中被取消（终态） | 不占 |

状态只允许向前转移，`version` 每次转移 +1 并随快照落盘。

## 3. 配额规则

配额维度为 `(callerId, group)`（`group` 缺省为 `default`），三项限制：

| 配置 | 含义 | 超限行为 |
| --- | --- | --- |
| `maxConcurrency` | 同一维度**同时执行**的任务数上限 | 保持排队（FIFO，队头阻塞公平） |
| `rateLimitPerSecond` | 同一维度每个**自然秒**最多**启动**的任务数（固定窗口） | 保持排队到下一秒；已消耗额度不回补 |
| `maxQueued` | 同一维度**排队中**（QUEUED + PENDING_RETRY）任务数上限 | 拒绝提交，返回 `QUOTA_EXHAUSTED` / HTTP 429 |

此外有一个全局硬上限 `worker-threads`：所有维度共享的执行线程数；
无线程空闲时任务不会在池内隐藏排队，而是回滚为 QUEUED 等下一轮派发。

**判定原子性**：所有状态转移与配额判定都在调度器的单一内部锁内串行完成，
`tryAcquireActive` 的“检查 + 占用”不可分割，杜绝 TOCTOU 越限执行。
派发采用每 key 队头阻塞：队头因配额或退避不能执行时，跳过该 key 后续任务，
不影响其它 `(caller, group)`。

## 4. 提交 / 幂等约定

- `taskId` 由调用方提供，是**幂等键**，必须非空。
- 提交成功先写 WAL（QUEUED 快照）再放入内存，保证“接受了就不会丢”。
- 相同 `taskId` 再次提交：
  - 不产生第二次执行，不重复占用配额；
  - 响应 `accepted=false, duplicate=true` 并返回任务当前状态（排队/执行中/终态皆然）。
- 处理器实现应尽量使副作用幂等；框架保证同一任务任意时刻只有一个执行实例、
  崩溃恢复后 RUNNING 任务最多再执行一次。

## 5. 重试 / 超时 / 取消约定

- **重试策略**：默认指数退避
  `delay = min(cap, base × multiplier^(retryIndex-1)) × (1 + jitter×rand[-1,1])`；
  `jitter=0` 时完全确定。重试次数 = 首次执行 + `maxRetries`。
- **错误分类**（结果写入 `errorReason` / `errorClass`，可解释）：
  - 抛 `NonRetryableTaskException` → `FAILED`，不重试，`errorReason` 前缀 `non-retryable error:`；
  - 普通异常 → 可重试，超过次数后 `FAILED`，前缀 `retryable error:`；
  - 执行超过 `timeoutMillis`（默认 30s，可按任务覆盖）→ `FAILED`，**不重试**，前缀 `execution timeout:`；
  - 调用取消（排队中或执行中）→ `CANCELLED`，原因前缀 `cancelled:`。
- **取消**：排队中在临界区内直接终结并释放排队名额；执行中通过中断停止；
  处理器应在关键步骤检查 `TaskContext.cancelled()`。
- **超时隔离**：超时定时器到点中断执行线程；即使任务不响应中断，框架也立即判
  `FAILED` 并释放配额，且不会启动第二个执行实例（待原执行体结束后线程回收）。
- 超时与取消优先于业务异常结果，四类失败原因互不混淆。

## 6. 持久化与重启恢复

- WAL 为本地 JSON Lines 文件（默认 `data/task-scheduler.wal`），每次状态转移追加一行任务快照，
  可选每次 `fsync`（`wal-fsync`）。
- 重启时按顺序回放，每个任务取最新快照：
  - `SUCCEEDED / FAILED / CANCELLED` 保持终态，不重新执行，不占配额；
  - `QUEUED / PENDING_RETRY` 重新计数为排队；退避到期时间是**绝对时间**，跨重启有效；
  - `RUNNING`（崩溃时在执行）重新置为 `QUEUED` 再执行一次，绝不静默丢失；
  - 配额计数按恢复结果 `reinitialize`，防止配额泄漏/超发。
- 崩溃导致最后一行写一半时，打开 WAL 会检测并**截断撕裂尾部**，此前完整记录不受影响。
- 运行期治理变更（配额调整 / 暂停 / 恢复）写入**独立的治理事件日志**（JSON Lines，
  默认 `<wal>.governance.jsonl`），与任务 WAL 采用相同的追加落盘与撕裂截断约定；
  启动时按顺序回放，每个作用域以最后一条事件为准。

## 7. 运行期治理（不重启调整配额 / 暂停恢复 / 运行态查询）

运行期治理在不改动提交、幂等、重试、取消、重启恢复语义的前提下，提供一层在线管控。
所有变更都在调度器的单一内部锁内完成“校验 + 落盘 + 生效”，与提交/派发天然串行，
数字与真实调度判定始终自洽。

### 7.1 作用域

| 入参 | 作用域 `scopeKind` | 含义 |
| --- | --- | --- |
| 同时给 `callerId` 与 `group` | `EXACT` | 精确某个调用方的某个任务组 |
| 只给 `callerId` | `CALLER` | 该调用方的**所有**任务组 |
| 只给 `group` | `GROUP` | **所有**调用方的该任务组 |
| 两者都不给 | — | 非法，返回 400 |

配额按**字段独立**解析，生效优先级（高 → 低）：
**运行期 EXACT ＞ 配置文件 EXACT ＞ 运行期 CALLER ＞ 运行期 GROUP ＞ 默认值**。
暂停为布尔继承：EXACT / CALLER / GROUP 三个作用域中**任一暂停**，该精确维度即暂停。

### 7.2 在线调整配额

三项配额均可单独调整（PATCH 语义，未给出的项保持不变），改完**立即参与后续调度判定**：

- **调小 `maxConcurrency`**：已在执行的任务**不会被中断、状态不回退**；只是之后不再按旧上限放行，
  待在执行任务自然结束后占用数降到新上限以内。
- **调大任意配额**：积压任务按**原 FIFO 公平顺序**尽快被放出去执行（队头阻塞规则不变）。
- **非法值**：任一配额传负数、或三项全为空、或未指定作用域，整次请求被拒绝（HTTP 400），
  **原来的有效值保持不变**，也不会被记为“最近一次操作”。
- `0` 表示不限制（与配置语义一致）。

### 7.3 暂停与恢复派发

- 暂停某维度后：该维度**新提交的任务照常受理并进入排队**，不会被拒绝或丢失；
  **已在执行的任务自然跑完**；只是派发器不再启动该维度的排队任务。
- 恢复后：排队任务（含 `PENDING_RETRY` 到期任务）按**原公平顺序**继续执行。
- 暂停/恢复与配额调整一样落入治理事件日志，**跨进程重启保持**：
  重启后若仍是暂停状态，**不会自动恢复放量**，必须显式调用恢复接口。

### 7.4 运行态查询

`GET /api/governance/status?callerId=...&group=...`（`group` 缺省 `default`）返回该精确维度：

| 字段 | 含义 |
| --- | --- |
| `active` | 正在执行数 |
| `queued` | 排队数（QUEUED + PENDING_RETRY） |
| `startedInCurrentSecond` | 当前自然秒已启动数 |
| `limits.{maxConcurrency,rateLimitPerSecond,maxQueued}` | 当前**生效**的三项配额 |
| `paused` | 是否处于暂停状态（含宽维度继承） |
| `lastChangeTimeMillis` | 最近一次调整/暂停操作时间（无则 -1） |
| `lastChangeDescription` | 最近一次操作内容（改了什么、是否暂停、原因、作用域） |

并发提交与并发调整同时发生时，所有数字都在同一把调度锁内一次性取齐：
已拒绝的任务从不入队，已结束/取消的任务必然同步释放计数，不会出现“已拒绝/已结束任务仍挂在排队统计”的矛盾。

### 7.5 与既有能力的兼容范围

- **提交 / 幂等**：完全不变；暂停期间重复提交依旧返回 `duplicate`，已受理任务不受影响。
- **重试 / 超时 / 取消**：完全不变；暂停只影响“是否启动新执行”，不影响在执行任务的失败收敛、
  退避计时与取消。恢复后退避到期任务照常派发。
- **重启恢复**：任务 WAL 的恢复规则不变；治理状态额外从治理日志回放，
  与任务恢复共同决定重启后的派发（暂停维度即使有排队任务也不放量）。
- **配置文件配额**：仍然有效，作为静态 EXACT 覆盖；运行期 EXACT 调整优先级更高并跨重启保持。

## 8. HTTP 接口

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /api/tasks` | 提交任务（body 见下） |
| `GET /api/tasks/{taskId}` | 查询单个任务（404 `TASK_NOT_FOUND`） |
| `GET /api/tasks` | 列出全部任务 |
| `POST /api/tasks/{taskId}/cancel?reason=...` | 取消，返回 `{taskId, cancelled}` |
| `POST /api/governance/quotas?callerId=..&group=..` | 运行期调整配额（PATCH，body 见下；400=非法/无作用域） |
| `POST /api/governance/pause?callerId=..&group=..&reason=..` | 暂停派发（二者至少给一个） |
| `POST /api/governance/resume?callerId=..&group=..&reason=..` | 恢复派发 |
| `GET /api/governance/status?callerId=..&group=..` | 查询精确维度运行态（`group` 缺省 default） |

提交请求：

```json
{
  "taskId": "batch-001",
  "callerId": "alice",
  "group": "daily-report",
  "taskType": "sample",
  "payload": "sleep:500",
  "timeoutMillis": 5000
}
```

提交响应三态可区分：

- 接受：HTTP 200 `{accepted:true, status:"QUEUED"}`
- 重复：HTTP 200 `{accepted:false, duplicate:true, status:"<当前状态>"}`
- 拒绝：HTTP 429 `{accepted:false, rejectReason:"QUOTA_EXHAUSTED"}`；
  未知任务类型 HTTP 404 `UNKNOWN_TASK_TYPE`；参数非法 HTTP 400 `INVALID_REQUEST`。

任务查询视图含 `status / attempts / version / result / errorReason / errorClass` 等字段。

运行期治理示例：

```bash
# 精确调整：只把 alice 的 daily-report 组并发上限改为 1（其余项不变），立即生效
curl -s -XPOST 'localhost:8080/api/governance/quotas?callerId=alice&group=daily-report' \
  -H 'Content-Type: application/json' \
  -d '{"maxConcurrency":1,"reason":"traffic-spike"}'
# -> {"scopeKind":"EXACT","callerId":"alice","group":"daily-report","paused":false,
#     "maxConcurrency":1,"rateLimitPerSecond":2,"maxQueued":100,"timeMillis":...,"reason":"traffic-spike"}

# 按调用方整体暂停（覆盖其全部任务组）；新任务照常受理排队，在跑的自然跑完
curl -s -XPOST 'localhost:8080/api/governance/pause?callerId=alice&reason=incident-freeze'
# 按任务组暂停（所有调用方的该组）
curl -s -XPOST 'localhost:8080/api/governance/pause?group=risky-batch'
# 恢复
curl -s -XPOST 'localhost:8080/api/governance/resume?callerId=alice'

# 查询运行态
curl -s 'localhost:8080/api/governance/status?callerId=alice&group=daily-report'
# -> {"callerId":"alice","group":"daily-report","active":1,"queued":3,
#     "startedInCurrentSecond":1,
#     "limits":{"maxConcurrency":1,"rateLimitPerSecond":2,"maxQueued":100},
#     "paused":true,"lastChangeTimeMillis":1727...,"lastChangeDescription":"paused=true ..."}
```

配额调整请求体字段均可省略（`maxConcurrency / rateLimitPerSecond / maxQueued / reason`）；
负数或三项全空返回 400 `INVALID_REQUEST` 且原值不变。

内置示例处理器 `sample`（`SampleTaskHandler`）的 payload 约定：
`sleep:N`（睡眠 N 毫秒并响应取消/超时）、`fail:msg`（可重试失败）、
`fatal:msg`（不可重试失败）、其它内容立即成功并 `echo` 回显。
自定义处理器实现 `TaskHandler`（`type()` 唯一）并注册为 Spring Bean 即可自动接入。

## 9. 关键配置项（`application.properties`，前缀 `task.scheduler`）

| 键 | 默认值 | 说明 |
| --- | --- | --- |
| `worker-threads` | 4 | 全局执行线程硬上限 |
| `default-max-concurrency` | 2 | 每维度默认并发上限 |
| `default-rate-limit-per-second` | 2 | 每维度默认每秒启动上限（0=不限） |
| `default-max-queued` | 100 | 每维度默认排队上限 |
| `default-timeout-millis` | 30000 | 默认执行超时（<=0 不限） |
| `default-max-retries` | 2 | 默认最大重试次数（不含首次） |
| `backoff-base-millis` | 200 | 退避基数 |
| `backoff-multiplier` | 2.0 | 退避乘数 |
| `backoff-cap-millis` | 10000 | 退避上限 |
| `backoff-jitter` | 0.0 | 抖动比例 [0,1)，0 表示确定 |
| `wal-file` | data/task-scheduler.wal | WAL 文件路径 |
| `wal-fsync` | true | 每次写 WAL 是否 fsync |
| `governance-file` | （空） | 治理事件日志路径；空则在 WAL 同目录生成 `<wal>.governance.jsonl` |
| `quotas.<caller|caller:group>.*` | — | 覆盖指定维度的三项配额（静态，优先级低于运行期 EXACT 调整） |

覆盖示例：

```properties
task.scheduler.quotas.alice.max-concurrency=1
task.scheduler.quotas.alice.rate-limit-per-second=1
task.scheduler.quotas.bob:batch.max-concurrency=3
task.scheduler.quotas.bob:batch.max-queued=200
```

## 10. 本地验证

环境：JDK 21+、Maven 3.9+。

```bash
# 全量测试（37 个：并发配额、重复提交、取消/超时、重试、重启恢复、WAL 完整性、
#           HTTP，以及运行期配额调整 / 暂停恢复 / 暂停态重启 / 并发提交与调整混沌）
mvn test

# 单个套件
mvn test -Dtest=ConcurrencyQuotaTests
mvn test -Dtest=RestartRecoveryTests
mvn test -Dtest=QuotaAdjustmentRuntimeTests
mvn test -Dtest=PauseResumeTests
mvn test -Dtest=GovernanceRestartTests
mvn test -Dtest=ConcurrentGovernanceTests
mvn test -Dtest=RateLimitRuntimeTests

# 启动服务后手工验证
mvn spring-boot:run
curl -s -XPOST localhost:8080/api/tasks -H 'Content-Type: application/json' -d '{
  "taskId":"t1","callerId":"alice","group":"g","taskType":"sample","payload":"sleep:500"}'
curl -s localhost:8080/api/tasks/t1
curl -s -XPOST 'localhost:8080/api/tasks/t1/cancel?reason=abort'

# 运行期治理：在线调配额 / 暂停 / 恢复 / 查运行态（无需重启，重启后仍保持）
curl -s -XPOST 'localhost:8080/api/governance/quotas?callerId=alice&group=g' \
  -H 'Content-Type: application/json' -d '{"maxConcurrency":1,"reason":"spike"}'
curl -s -XPOST 'localhost:8080/api/governance/pause?callerId=alice&reason=hold'
curl -s 'localhost:8080/api/governance/status?callerId=alice&group=g'
curl -s -XPOST 'localhost:8080/api/governance/resume?callerId=alice'
```

新增治理相关测试套件与关注点：

| 套件 | 覆盖场景 |
| --- | --- |
| `QuotaAdjustmentRuntimeTests` | 调小不中断在执行/后续按新上限放行；调大按 FIFO 放量；非法值拒绝且原值不变；PATCH 合并 |
| `PauseResumeTests` | 暂停期受理排队、在跑自然跑完、恢复后 FIFO；按 caller / 按 group 维度隔离 |
| `GovernanceRestartTests` | 暂停状态、运行期配额跨重启保持，重启不自动放量，显式恢复后收敛 |
| `ConcurrentGovernanceTests` | 并发提交 + 并发调整混沌：上限不突破、终态稳定收敛、统计自洽；非法并发调整不污染生效值 |
| `RateLimitRuntimeTests` | 每秒启动上限在线调小/调大立即生效，本秒启动计数可观测 |
| `GovernanceControllerIntegrationTests` | 治理 HTTP 端点：调整/暂停/恢复/状态查询、400 非法值与无作用域 |

测试日志（单测控制台与 surefire 报告）按要求打印：
`[提交标识=...]`、`[配额判定] active=x/上限, windowStarts=x/s, queued=x`、
`[暂停状态] ... paused=true/false`、
以及每次 `状态->QUEUED/RUNNING/PENDING_RETRY/SUCCEEDED/FAILED/CANCELLED` 的变化与配额释放。

## 11. 代码结构

```
config/      SchedulerProperties、Spring 装配（默认/覆盖配额、处理器注册、治理日志）
model/       TaskRecord（WAL 快照）、TaskStatus、RejectReason、TaskException
quota/       QuotaManager（并发槽位 + 固定窗口速率 + 排队计数 + 跨作用域生效配额/暂停解析）
governance/  GovernanceScope/Event/Status/ChangeResult、GovernanceStore、FileGovernanceStore
retry/       RetryPolicy、指数退避实现、错误分类、NonRetryableTaskException
handler/     TaskHandler/TaskContext、注册表、内置 sample 处理器
store/       TaskStore、WalTaskStore（JSON Lines WAL + 撕裂截断恢复）
service/     TaskSchedulerService（提交/派发/执行/重试/超时/取消/恢复/运行期治理编排）
web/         TaskController、GovernanceController、GlobalExceptionHandler、DTO
```
