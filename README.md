# 配额感知的本地异步任务调度器

在 Spring Boot（Java 21）工程内提供一套**纯本地**的配额感知异步任务调度能力：
多调用方并发提交批处理任务时，执行结果、资源占用与失败原因确定且可解释。
**不依赖任何外部消息队列、缓存、数据库或调度服务**——状态持久化只使用本地 JSON Lines WAL 文件。

## 1. 能力概览

- 异步任务的提交、排队与执行；按 `callerId + group`（调用方/任务组）两个维度限制
  **并发度**、**单位时间启动量**与**排队长度**。
- 配额不足时任务**排队**；排队也满时**拒绝**。排队 / 拒绝 / 超时 / 取消是四种可区分、稳定的结果。
- 提交幂等：相同 `taskId` 的重复提交（含高并发下的同时提交）只接受一次、只执行一次、只占一份配额。
- 状态持久化到本地 WAL；进程重启后终态不回退、排队任务不丢失、执行中崩溃的任务重新排队、配额不泄漏。
- 失败任务支持**指数退避重试**与终止策略；不可重试错误、取消、超时立即终止，绝不无限重试或重复执行。
- **运行期治理**：无需重启即可按调用方/任务组调整三项配额、暂停/恢复派发，
  并随时查询各维度运行态；治理状态持久化，重启后保持。

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

## 4. 运行期治理（配额热调整 / 暂停恢复 / 运行态查询）

在不动原有提交、幂等、重试、取消与重启恢复语义的前提下，提供三个运行期治理能力，
全部通过 HTTP 接口触发，**立即生效并持久化**（治理 WAL，见第 7 节）。

### 4.1 配额热调整

`PUT /api/governance/quota`，body：

```json
{"callerId":"alice","group":"daily-report","maxConcurrency":4,"rateLimitPerSecond":0,"maxQueued":200}
```

- 三项配额必须**全部给出且 >= 0**（0 表示不限，`group` 缺省为 `default`）；
  改完立即参与后续调度判定。
- **调小并发上限**：已在执行的任务不被中断、状态不回退，只是不再按旧上限放行新任务；
  执行中任务自然结束后，占用降到新上限以下才会继续派发。
- **调大上限**：积压的排队任务按原有队头阻塞 FIFO 顺序尽快放出。
- **非法值**（缺项 / 任一项为负 / callerId 为空）：HTTP 400 `INVALID_REQUEST`，
  原有生效值与已持久化的治理状态都保持不变。

### 4.2 暂停与恢复派发

`POST /api/governance/pause` / `POST /api/governance/resume`，body：`{"callerId":"alice","group":"g"}`。

- 暂停期间：该维度**新提交照常受理并排队**（不拒绝、不丢失），执行中任务自然跑完，
  只是不再启动新任务；排队上限等既有配额判定不受影响。
- 恢复后：排队任务按原公平顺序继续执行。
- 暂停/恢复状态与配额调整一样**跨进程重启保留**，重启不会自动恢复放量。

### 4.3 运行态查询

`GET /api/governance/status?callerId=alice&group=g` 查单个维度；不带参数列出全部已知维度。返回：

```json
{
  "callerId": "alice", "group": "g",
  "maxConcurrency": 4, "rateLimitPerSecond": 0, "maxQueued": 200,
  "active": 2, "queued": 5, "startedInCurrentWindow": 1,
  "paused": false,
  "lastOperation": "ADJUST_QUOTA",
  "lastOperationAtEpochMillis": 1759550000000,
  "lastOperationDetail": "maxConcurrency=4, rateLimitPerSecond=0, maxQueued=200"
}
```

计数（`active` / `queued` / `startedInCurrentWindow`）与调度判定在同一临界区内读取：
并发提交与并发调整同时进行时，视图与实际判定一致，
已被拒绝或已结束的任务不会残留在排队统计里。

### 4.4 与既有能力的兼容范围

- 治理操作只影响**后续调度判定**；任务状态机、幂等键、重试退避、取消/超时语义完全不变。
- 运行期配额覆盖持久化在治理 WAL 中，优先级高于 `application.properties` 的
  `task.scheduler.quotas.*` 静态配置（重启后先应用静态配置，再回放治理 WAL）。
- 治理 WAL 与任务 WAL 是**两个独立文件**，互不影响撕裂截断与回放逻辑。

## 5. 提交 / 幂等约定

- `taskId` 由调用方提供，是**幂等键**，必须非空。
- 提交成功先写 WAL（QUEUED 快照）再放入内存，保证“接受了就不会丢”。
- 相同 `taskId` 再次提交：
  - 不产生第二次执行，不重复占用配额；
  - 响应 `accepted=false, duplicate=true` 并返回任务当前状态（排队/执行中/终态皆然）。
- 处理器实现应尽量使副作用幂等；框架保证同一任务任意时刻只有一个执行实例、
  崩溃恢复后 RUNNING 任务最多再执行一次。

## 6. 重试 / 超时 / 取消约定

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

## 7. 持久化与重启恢复

- WAL 为本地 JSON Lines 文件（默认 `data/task-scheduler.wal`），每次状态转移追加一行任务快照，
  可选每次 `fsync`（`wal-fsync`）。
- 重启时按顺序回放，每个任务取最新快照：
  - `SUCCEEDED / FAILED / CANCELLED` 保持终态，不重新执行，不占配额；
  - `QUEUED / PENDING_RETRY` 重新计数为排队；退避到期时间是**绝对时间**，跨重启有效；
  - `RUNNING`（崩溃时在执行）重新置为 `QUEUED` 再执行一次，绝不静默丢失；
  - 配额计数按恢复结果 `reinitialize`，防止配额泄漏/超发。
- 崩溃导致最后一行写一半时，打开 WAL 会检测并**截断撕裂尾部**，此前完整记录不受影响。
- 治理状态（运行期配额覆盖 + 暂停标记）持久化在独立的治理 WAL
  （默认 `data/task-scheduler.governance.wal`，可用 `governance-file` 调整）：
  同样的 JSON Lines 追加 + 撕裂截断策略，重启后按维度回放最新一条，
  配额调整与暂停状态都不会因重启丢失。

## 8. HTTP 接口

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /api/tasks` | 提交任务（body 见下） |
| `GET /api/tasks/{taskId}` | 查询单个任务（404 `TASK_NOT_FOUND`） |
| `GET /api/tasks` | 列出全部任务 |
| `POST /api/tasks/{taskId}/cancel?reason=...` | 取消，返回 `{taskId, cancelled}` |
| `PUT /api/governance/quota` | 运行期调整某维度三项配额（非法值 400 且原值不变） |
| `POST /api/governance/pause` | 暂停某维度派发（新任务照常排队） |
| `POST /api/governance/resume` | 恢复某维度派发 |
| `GET /api/governance/status[?callerId&group]` | 查询单个或全部维度的运行态 |

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
| `governance-file` | data/task-scheduler.governance.wal | 治理状态（配额热调整/暂停）WAL 路径 |
| `quotas.<caller|caller:group>.*` | — | 覆盖指定维度的三项配额（静态初始值，运行期调整优先于它） |

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
# 全量测试（32 个：并发配额、重复提交、取消/超时、重试、重启恢复、WAL 完整性、
# 运行期治理、HTTP）
mvn test

# 单个套件
mvn test -Dtest=ConcurrencyQuotaTests
mvn test -Dtest=RestartRecoveryTests
mvn test -Dtest=RuntimeGovernanceTests

# 启动服务后手工验证
mvn spring-boot:run
curl -s -XPOST localhost:8080/api/tasks -H 'Content-Type: application/json' -d '{
  "taskId":"t1","callerId":"alice","group":"g","taskType":"sample","payload":"sleep:500"}'
curl -s localhost:8080/api/tasks/t1
curl -s -XPOST 'localhost:8080/api/tasks/t1/cancel?reason=abort'

# 运行期治理：热调配额、暂停/恢复、查运行态（重启后状态保留）
curl -s -XPUT localhost:8080/api/governance/quota -H 'Content-Type: application/json' -d '{
  "callerId":"alice","group":"g","maxConcurrency":1,"rateLimitPerSecond":0,"maxQueued":50}'
curl -s -XPOST localhost:8080/api/governance/pause -H 'Content-Type: application/json' -d '{
  "callerId":"alice","group":"g"}'
curl -s 'localhost:8080/api/governance/status?callerId=alice&group=g'
curl -s -XPOST localhost:8080/api/governance/resume -H 'Content-Type: application/json' -d '{
  "callerId":"alice","group":"g"}'
curl -s localhost:8080/api/governance/status
```

测试日志（单测控制台与 surefire 报告）按要求打印：
`[提交标识=...]`、`[配额判定] active=x/上限, windowStarts=x/s, queued=x`、
`[治理] 配额调整/暂停派发/恢复派发 ...`、`[运行态] active=x queued=x ... paused=...`，
以及每次 `状态->QUEUED/RUNNING/PENDING_RETRY/SUCCEEDED/FAILED/CANCELLED` 的变化与配额释放。

## 11. 代码结构

```
config/      SchedulerProperties、Spring 装配（默认/覆盖配额、处理器注册、治理存储）
model/       TaskRecord（WAL 快照）、TaskStatus、RejectReason、TaskException
quota/       QuotaManager（并发槽位 + 固定窗口速率 + 排队计数 + 暂停标记）、QuotaStatus
governance/  GovernanceRecord、GovernanceStore（治理状态 JSON Lines WAL + 撕裂截断恢复）
retry/       RetryPolicy、指数退避实现、错误分类、NonRetryableTaskException
handler/     TaskHandler/TaskContext、注册表、内置 sample 处理器
store/       TaskStore、WalTaskStore（JSON Lines WAL + 撕裂截断恢复）
service/     TaskSchedulerService（提交/派发/执行/重试/超时/取消/恢复/运行期治理编排）
web/         TaskController、GovernanceController、GlobalExceptionHandler、DTO
```
