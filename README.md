# 配额感知的本地异步任务调度器

在 Spring Boot 工程内实现的**单机、无外部依赖**（不依赖 MQ / Redis / 外部调度服务）的异步批处理任务调度能力。
多个调用方并发提交任务时，执行结果、资源占用与失败原因**确定且可解释**：

- 按**调用方**与**全局**两个作用域限制：并发度、排队长度、单位时间执行启动量（令牌桶限速）；
- 配额不足时**排队或确定性拒绝**，绝不越限执行；排队 / 拒绝 / 超时 / 取消结果互不混淆；
- 重复提交（同一 `caller + submitKey`）幂等：不重复执行、不重复占用配额；
- 全部状态变化**先持久化后生效**（每任务一个 JSON 文件，临时文件 + 原子 rename），
  进程重启后任务不丢失、状态不回退、配额不泄漏；
- 失败任务按指数退避重试，原因码区分「可重试异常 / 不可重试 / 超时 / 取消 / 重启中断」。

---

## 1. 快速开始

```bash
mvn spring-boot:run
```

提交一个任务：

```bash
curl -X POST http://localhost:8080/api/tasks \
  -H 'Content-Type: application/json' \
  -d '{"caller":"team-a","submitKey":"batch-0001","taskType":"demo","payload":"hello"}'
```

> `taskType` 通过实现 `TaskHandler` 接口并注册为 Spring Bean 进行扩展：
>
> ```java
> @Component
> public class DemoHandler implements TaskHandler {
>     public String type() { return "demo"; }
>     public void handle(String payload, TaskContext ctx) {
>         // 长任务应周期检查 ctx.isCancelled()，及时响应取消/超时
>     }
> }
> ```

---

## 2. HTTP 接口

| 方法 & 路径 | 说明 | 关键响应 |
|---|---|---|
| `POST /api/tasks` | 提交任务（body: `caller/submitKey/taskType/payload`） | `200` + `verdict=START|ENQUEUE`，`duplicate=true|false`；配额耗尽 `429` + `verdict=REJECT` |
| `GET /api/tasks/{id}` | 按任务 ID 查询（含每次尝试明细与失败原因） | `404` 不存在 |
| `GET /api/tasks/by-key/{caller}/{submitKey}` | 按幂等键查询原任务 | `404` 不存在 |
| `POST /api/tasks/{id}/cancel` | 取消任务 | 排队/重试等待中立即 `CANCELLED`；运行中返回当前 `RUNNING`，任务协作式停止后变 `CANCELLED` |
| `GET /api/quota?caller=team-a` | 配额视图（不传 caller 返回全局） | `running/queued/maxConcurrency/maxQueued/availableTokens` |

提交响应示例（被拒绝）：

```json
{
  "taskId": "…", "status": "REJECTED", "verdict": "REJECT",
  "duplicate": false, "rejectReason": "CALLER_QUEUE_FULL"
}
```

错误响应统一为 `{"code","message"}`，状态码：`400 BAD_REQUEST` / `404 NOT_FOUND` /
`429 QUOTA_EXHAUSTED` / `500 STORE_FAILURE|INTERNAL_ERROR`。

---

## 3. 任务状态机

```
                 ┌──────────(提交)──────────┐
 PENDING ──┬──> RUNNING ──成功────────────> COMPLETED     （终态）
           │      │
           │      ├──可重试失败──> WAITING_RETRY ──退避到期──> RUNNING …
           │      │
           │      ├──重试耗尽/不可重试──────────────────> FAILED      （终态）
           │      ├──超时（看门狗中断）─────────────────> TIMED_OUT   （终态，不重试）
           │      └──取消（协作式）─────────────────────> CANCELLED   （终态）
           └──> QUEUED ──派发时配额满足──> RUNNING
                 QUEUED/WAITING_RETRY ──取消───────────> CANCELLED
 配额不足 ─> REJECTED（提交即终态，携带 CALLER_QUEUE_FULL / GLOBAL_QUEUE_FULL）
```

- 终态：`COMPLETED / FAILED / CANCELLED / TIMED_OUT / REJECTED`，不再变化、重启不回退。
- 每次尝试（Attempt）都记录：序号、起止时间、成功与否、失败原因码与异常信息；
  最终 `failure` 字段给出**稳定原因码**与人类可读说明。

### 失败原因码（`FailureCode`）

| code | 含义 | 是否继续 |
|---|---|---|
| `HANDLER_EXCEPTION` | 处理器抛出普通异常 | 退避重试，直到 `maxAttempts` 后 `FAILED` |
| `NON_RETRYABLE` | 抛出 `NonRetryableTaskException` | 立即 `FAILED`，不重试 |
| `ATTEMPT_TIMEOUT` | 单次执行超过 `attempt-timeout-millis` | 立即 `TIMED_OUT`，不重试，中断工作线程 |
| `CANCELLED` | 调用方取消 | 立即 `CANCELLED` |
| `RESTART_INTERRUPTED` | 进程重启时任务仍在 RUNNING | 默认 `FAILED`；配置开启后退避重试（见 §6） |
| `CALLER_QUEUE_FULL` / `GLOBAL_QUEUE_FULL` | 提交时队列已满 | 立即 `REJECTED`（HTTP 429） |
| `INVALID_REQUEST` / `UNKNOWN_TASK_TYPE` | 参数非法 / 无对应处理器 | 拒绝提交（HTTP 400） |

---

## 4. 配额规则

每个作用域（**全局** + **每个调用方**）维护三类资源，判定与记账在同一把锁内**原子**完成：

| 资源 | 字段 | 占用时机 | 释放时机 |
|---|---|---|---|
| 并发槽位 | `running / maxConcurrency` | 任务**开始执行**（含排队任务被派发） | 尝试成功 / 失败终态 / 超时 / 取消 |
| 队列槽位 | `queued / maxQueued` | 提交后无法立即运行而排队 | 任务被派发（转运行）或排队中被取消 |
| 速率令牌 | token bucket `rateLimitPerSecond` | 任务**开始执行**时消耗 1 个 | 按秒惰性补充，桶容量=每秒限额 |

提交判定顺序（全部不满足才拒绝）：

1. 调用方并发槽 + 全局并发槽 + 双方速率令牌都满足 → **立即 START**；
2. 否则调用方队列槽 + 全局队列槽满足 → **ENQUEUE**，由派发线程按到期时间（排队时间 / 重试 `nextRunAt`）轮询提升；
3. 否则 → **REJECT**：先判调用方队列是否满（`CALLER_QUEUE_FULL`），再判全局（`GLOBAL_QUEUE_FULL`）。

**保证**：`running ≤ maxConcurrency`、`queued ≤ maxQueued`、任意秒内启动执行的任务数 ≤ 速率限额，
所有上限对全局和每个调用方**同时**成立。重试退避中的任务不占队列槽也不占运行槽，
但重试重新启动时仍需通过并发槽 + 令牌判定（重试不会挤占限额）。

### 按调用方覆盖

```properties
scheduler.callers.team-a.max-concurrency=4
scheduler.callers.team-a.max-queued=100
scheduler.callers.team-a.rate-limit-per-second=10
scheduler.callers.team-a.max-attempts=5
```

未配置的调用方使用 `scheduler.default-*` 默认值。

---

## 5. 重试与幂等约定

- 退避公式：`delay = min(backoff-base-millis * backoff-multiplier^(attempt-1), max-backoff-millis)`，默认 200ms 起、2 倍递增、封顶 10s。
- `maxAttempts` **含首次执行**；`1` 表示不重试。状态 `WAITING_RETRY` 携带 `nextRunAt` 与最近一次失败原因。
- 处理器想表达「不要重试」请抛 `NonRetryableTaskException`。
- **幂等键**：`submitKey` 非空时，`(caller, submitKey)` 全局唯一。并发/重复提交只有一个请求真正创建任务，
  其余得到 `duplicate=true` 和同一个 `taskId`，不产生第二次执行、不占第二份配额。
  不传 `submitKey` 表示不去重（每次提交都是新任务）。
- **业务副作用幂等性由处理器负责**。同一任务的重试 / 重启后重试可能导致同一业务操作被执行多次，
  处理器应基于业务 ID 做去重；调度器保证的是「同一次提交不会产生两条任务」。
- 超时 / 取消通过中断工作线程 + `TaskContext.isCancelled()` 通知，处理器必须配合及时退出，
  不会被无限重试（超时不重试；取消不重试）。

---

## 6. 重启恢复（持久化语义）

- 存储目录：`scheduler.data-dir`（默认 `data/tasks`），每个任务 `<taskId>.json`；
  写入为「写临时文件 → 原子 rename」，任意时刻崩溃只会留下旧版本或新版本，不会出现半文件。
- 启动时加载全部任务文件并重建幂等索引与配额计数：

| 崩溃前状态 | 恢复动作 |
|---|---|
| `QUEUED / PENDING` | 重新入队，继续等待派发（不丢失） |
| `WAITING_RETRY` | 按原 `nextRunAt` 继续等待，已用尝试次数保留 |
| `RUNNING` | 默认标记 `FAILED(RESTART_INTERRUPTED)`；`scheduler.retry-running-tasks-on-restart=true` 时退避后重试一次（**要求处理器幂等**） |
| 任意终态 | 原样加载，不回退、不计数 |

- 崩溃进程残留的运行槽位**一律归零重建**：运行计数只由恢复后仍在执行的任务决定（崩溃瞬间无存活执行），
  因此不会出现「槽位永远被占」的配额泄漏；排队计数按磁盘上的 QUEUED 任务精确重建。

---

## 7. 关键配置项（`application.properties`）

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `scheduler.data-dir` | `data/tasks` | 任务文件目录 |
| `scheduler.global-max-concurrency` | 8 | 全局并发上限（= 工作线程数） |
| `scheduler.global-max-queued` | 200 | 全局排队上限 |
| `scheduler.global-rate-limit-per-second` | 20 | 全局每秒执行启动量 |
| `scheduler.default-max-concurrency` | 2 | 调用方默认并发上限 |
| `scheduler.default-max-queued` | 50 | 调用方默认排队上限 |
| `scheduler.default-rate-limit-per-second` | 5 | 调用方默认每秒执行启动量 |
| `scheduler.attempt-timeout-millis` | 30000 | 单次尝试超时，超时终态 `TIMED_OUT` |
| `scheduler.default-max-attempts` | 3 | 默认最大尝试次数（含首次） |
| `scheduler.backoff-base-millis` | 200 | 退避基数（ms） |
| `scheduler.backoff-multiplier` | 2.0 | 退避倍数 |
| `scheduler.max-backoff-millis` | 10000 | 退避上限（ms） |
| `scheduler.retry-running-tasks-on-restart` | false | 重启中断的 RUNNING 任务是否重试 |
| `scheduler.tick-millis` | 10 | 派发扫描间隔（ms） |
| `scheduler.callers.<name>.*` | — | 按调用方覆盖上述并发/队列/速率/尝试次数 |

---

## 8. 本地验证

```bash
# 全量单测 + Spring Boot 集成测试（21 个用例）
mvn test

# 只跑某一类场景
mvn test -Dtest=QuotaConcurrencyTest     # 并发上限/队列耗尽/速率限制
mvn test -Dtest=DedupSubmitTest          # 20 线程并发重复提交
mvn test -Dtest=CancelTimeoutTest        # 排队取消/运行取消/超时
mvn test -Dtest=RetryTest                # 退避重试/耗尽/不可重试
mvn test -Dtest=RestartRecoveryTest      # 进程重启恢复（模拟 kill -9）
mvn test -Dtest=WebIntegrationTest       # HTTP 全链路 + 429/400/404
```

测试日志固定打印：**提交标识**（`caller / submitKey / taskId`）、**配额判定**
（`START / ENQUEUE / REJECT + reason`、占用快照）与**状态变化**
（`旧状态 -> 新状态`、尝试序号与原因码），便于对执行路径做确定性审计。

测试用临时目录（`Files.createTempDirectory`），不会污染工作目录；应用自身运行时的 `data/` 已在 `.gitignore` 中忽略。

---

## 9. 包结构

```
scheduler/
├── domain/   枚举与值对象（TaskStatus / FailureCode / RetryPolicy / TaskRecord / SubmitResult / QuotaSnapshot …）
├── quota/    QuotaManager 接口 + InMemoryQuotaManager（槽位 + 令牌桶）
├── store/    TaskStore 接口 + FileTaskStore（原子文件持久化 + 幂等索引）
├── handler/  TaskHandler / TaskContext / NonRetryableTaskException / 注册表
├── service/  TaskSchedulerService 接口 + DefaultTaskSchedulerService（状态机/派发/重试/超时/恢复）
├── config/   SchedulerProperties + SchedulerConfig（Bean 装配）
└── web/      TaskController / DTO / 统一异常处理
```
