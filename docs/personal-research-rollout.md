# 个人 A 股投研：迁移与功能开关

个人投研能力采用显式数据库迁移和分阶段 Feature Flag。所有新增开关默认关闭；不配置时保持现有分析、持仓、报告和通知行为。

## 数据库迁移

只读检查不会创建数据库、表或 `DatabaseManager`：

```bash
python -m src.migrations --check
```

显式应用所有待执行迁移：

```bash
python -m src.migrations --apply
```

也可以指定隔离数据库并调整等待迁移锁的时间：

```bash
python -m src.migrations --check --database-url sqlite:////data/dsa/stock_analysis.db
python -m src.migrations --apply --database-url sqlite:////data/dsa/stock_analysis.db --lock-timeout 30
```

`--check` 返回码：当前版本为 `0`、存在待执行迁移为 `2`、数据库包含当前代码不认识或乱序的版本为 `3`。`--apply` 成功为 `0`，失败为 `1`。输出为 JSON，可用于部署前置检查。

迁移按版本固定排序、幂等执行，并通过数据库旁的 `.migration.lock` 在整个迁移序列期间进行同宿主单写者串行化。每个版本只在其 DDL/回填全部成功后写入 marker；如进程中断，下次 `--apply` 会安全重放未标记的幂等版本。

非 Compose 旧部署默认使用 `DATABASE_MIGRATION_MODE=auto`，应用初始化会复用同一把锁自动收敛 schema，保持旧启动方式可用。Docker Compose 则固定注入 `DATABASE_MIGRATION_MODE=explicit`：仅一次性 `migrator` 允许执行 DDL/回填，API、Worker 和 Scheduler 启动时只读验证版本，发现待迁移会直接拒绝启动。

## Durable Worker 与调度所有权

`DURABLE_JOBS_ENABLED=true` 时，API 和 Scheduler 只写入 `analysis_jobs`；只有独立的 `python -m src.services.durable_worker` 进程 claim 并执行版本化任务。每日分析使用一个 `scheduled_analysis` 编排任务，事件监控和 Decision Signal Outcome 也写入同一队列；活动 dedupe key 会合并重叠 tick。关闭开关时仍执行原有进程内路径。

任务状态和运行事件分别持久化到 `analysis_jobs` 与 `job_events`。Worker 每 15 秒续租，租约为 90 秒；一次初始执行加最多三次重试。Worker 被终止后，旧 lease token 无法再写入报告、Decision Signal 或任务终态，新 Worker 可以恢复过期任务。SSE 使用全局事件序号并支持 `Last-Event-ID` 或 `last_event_id` 续传；事件保留 30 天且每任务最多 1000 条。取消正在阻塞的外部请求不会强杀线程，只会在请求超时后的下一个安全边界生效。

Compose 只把 Worker 放入 durable profile；现有 `analyzer` 始终是唯一 Scheduler owner。切换时先启动 Worker，再重建 `analyzer`/`server`：

```bash
docker compose -f ./docker/docker-compose.yml --profile durable up -d worker
docker compose -f ./docker/docker-compose.yml up -d --force-recreate analyzer server
```

开启 durable 后，`analyzer` 在注册任何 daily/background task 前等待 `DURABLE_WORKER_ID` 对应的数据库心跳处于 `idle`/`busy` 且新鲜；超时会退出并等待容器重启。`server` 不依赖 Worker 心跳。Compose 不定义第二个 `scheduler` 服务，避免任何 profile 组合产生两个 owner。

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `DURABLE_WORKER_ID` | `compose-worker` | Compose 单 Worker 的稳定实例 ID |
| `DURABLE_WORKER_CONCURRENCY` | `3` | Worker 内并发 handler 数，允许 1–64 |
| `DURABLE_WORKER_HEALTH_MAX_AGE_SECONDS` | `45` | Scheduler 启动前允许的最大心跳年龄 |
| `DURABLE_WORKER_STARTUP_TIMEOUT_SECONDS` | `120` | Analyzer 等待 Worker 的最长启动秒数 |

可在容器或同一运行环境中执行只读心跳检查：

```bash
python -m src.services.durable_worker --healthcheck --worker-id compose-worker --max-heartbeat-age 45
```

## Durable 通知 Outbox

Durable Worker 内的通知不会直接发送，而是按目标渠道逐行写入 `notification_outbox`，再由同一 Worker 进程中的串行 Dispatcher 投递。成功渠道不会因其它渠道失败而重发；可明确判定为未发送的临时失败遵守 `Retry-After` 和重试上限。外部渠道已经接受、但进程在写回 `sent` 前崩溃时，记录会进入终态 `delivery_unknown`，不会自动重发。这一策略优先避免重复通知，代价是极少数不确定通知需要人工审计且可能丢失。

Outbox 只保存投递所需的最小目标。静态渠道凭据始终从运行时配置加载；Webhook、Token、原始 Bot 消息不进入任务或 Outbox。通知目标配置在入队后发生变化时，Dispatcher 会拒绝改投到新目标。

## Bot 持久任务

Flag 关闭时，Bot 保持原来的同步、`TaskService` 和后台线程路径。Flag 开启时，`/analyze`、`/batch`、`/market`、`/ask`、`/research` 只提交版本化任务并立即返回任务 ID，最终结果经 Outbox 回推。飞书和 Telegram 仅保存 `platform`、`chat_id`、`message_id`；钉钉临时 Session Webhook 无法安全跨进程保存，因此会在提交前明确拒绝且不创建任务。

## 备份与恢复

生产切换前使用 [SQLite 在线备份、校验与恢复](operations/sqlite-backup.md) 创建带 SHA-256、Schema/Index hash、核心表计数、`quick_check` 和外键检查的备份对。PR1 默认核心表包括任务、事件、Outbox 和组件健康表，恢复演练不得只验证业务报告表。

## 功能开关和依赖

| 配置 | 默认值 | 前置依赖 |
| --- | --- | --- |
| `PERSONAL_RESEARCH_ENABLED` | `false` | 无，个人投研总开关 |
| `DURABLE_JOBS_ENABLED` | `false` | 无，可先独立启用验证兼容性 |
| `TUSHARE_RESEARCH_ENABLED` | `false` | Personal Research、Durable Jobs |
| `RESEARCH_FACTORS_ENABLED` | `false` | Personal Research、Tushare Research |
| `RESEARCH_EVIDENCE_ENABLED` | `false` | Personal Research、Research Factors |
| `RESEARCH_DEBATE_ENABLED` | `false` | Personal Research、Research Evidence |
| `RESEARCH_THESIS_ENABLED` | `false` | Personal Research、Research Evidence |
| `DECISION_OUTCOME_V2_ENABLED` | `false` | Personal Research、Research Factors |
| `PORTFOLIO_POLICY_GATE_MODE` | `off` | `shadow`/`enforce` 还要求 Personal Research、Factors、Evidence |

`DECISION_OUTCOME_V2_ENABLED` 只控制个人投研 Outcome v2 的分阶段能力；现有 `decision-signal-v1` 后台维护由 `DECISION_SIGNAL_OUTCOME_ENABLED` 独立控制，默认开启，但只有通过 `SCHEDULE_ENABLED=true` 或 `--schedule` 启动的唯一 Scheduler owner 才会周期执行。关闭 v2 不会关闭 v1，反之亦然。

建议生产启用顺序：

1. 先运行迁移检查和迁移应用。
2. 启用 `DURABLE_JOBS_ENABLED` 并验证旧任务契约。
3. 依次启用 Personal Research、Tushare Research、Factors、Evidence。
4. 按需启用 Debate、Thesis、Outcome v2。
5. Policy Gate 先使用 `shadow` 观察，验收后改为 `enforce`。

任何不完整依赖组合都会在运行时配置加载阶段拒绝启动，并在 Web 配置保存阶段返回结构化校验错误。回滚时按相反顺序关闭开关；数据库迁移为追加式，关闭开关不要求降级数据库。
