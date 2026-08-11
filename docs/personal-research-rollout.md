# 个人 A 股投研：迁移与功能开关

个人投研能力采用显式数据库迁移和分阶段 Feature Flag。所有新增开关默认关闭；不配置时保持现有分析、持仓、报告和通知行为。

## 原始批准范围与阶段映射

本文件以最初批准的“DSA 个人 A 股投研二开与生产验证计划”PR0–PR6 为交付真源。后续开发过程中曾把 Evidence 和 Bounded Debate 分别简称为“PR3”和“PR4”；这只是实现批次编号，不代表原计划的同名阶段已经整体完成。为避免再次发生范围漂移，正式验收统一使用下表编号：

| 原计划阶段 | 交付范围 | 当前基线状态 |
| --- | --- | --- |
| PR0 | 现网基线、迁移框架、readiness、Feature Flag 与发布骨架 | 已实现、测试并部署 |
| PR1 | Durable Job、Lease/Heartbeat、追踪、Outbox、Bot 持久任务与 SQLite 备份 | 已实现和测试；生产功能开关尚未启用 |
| PR2 | Tushare 统一 Provider、不可变 Dataset/Research Snapshot、确定性 Value/Trend/Catalyst/Risk 因子 | 已实现和测试；生产完整研究链尚未启用 |
| PR3 | 增强关注池、持仓 Opening/Reconciliation、研究预算、Decision Signal 扩展、Portfolio Policy Gate Shadow/Enforce | 代码、迁移、API、Web 与离线测试已落地；生产开关仍为 off，7 个交易日 Shadow 尚未完成 |
| PR4 | typed Evidence/Claim、个人 Skill、任务模式、按需 Debate、Verifier/Judge、Thesis 与确定性 Gate 联动 | 代码、迁移、只读 API/Web 与离线测试已落地；生产开关与 canary 尚未完成 |
| PR5 | Decision Outcome v2：T+1 可执行性、5/10/20d、MFE/MAE、基准/行业超额和校准 | 代码、迁移、durable runtime、独立 API/Web、校准与离线测试已落地；生产数据 canary 待分阶段启用后确认 |
| PR6 | Web、调度、通知、OpenAPI、部署与生产验收收口 | Web、Scheduler 只入队、可选低敏通知、OpenAPI 和文档契约已落地；生产部署与 canary 验收记录尚待完成 |

只有 PR0–PR6 的代码、迁移、离线测试、Web/Docker 验证、生产候选验收、分阶段启用和验收记录全部闭合后，才能标记整个计划完成。代码部署成功不能替代功能启用与业务 canary；Policy Gate 必须先完成 7 个交易日 Shadow 并确认差异报告，才允许切换 Enforce。

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

## PR2 Tushare 研究数据与确定性因子

启用 `TUSHARE_RESEARCH_ENABLED` 后，所有 Tushare Pro 请求统一经过 Durable Worker 内的共享 Provider。账号级滚动总桶默认 450 次/分钟，实际在途请求最多 2；端点限额只能进一步收紧，不能与总桶叠加放宽。Worker 启动时会在 SQLite 旁非阻塞获取 `*.tushare-owner.lock` 跨进程独占锁，第二个 Worker 直接拒绝启动；锁文件保持 0 字节，进程崩溃后的残留文件本身不代表锁仍被占用。`TUSHARE_TOKEN` 缺失会被启动依赖检查阻断，429 会保留 `Retry-After`，鉴权、权限、超时、连接、响应格式和业务错误分别记录，API、Scheduler 与同步筛选路径不得绕过 Worker 直连。

共享 Provider 在线程锁内累计真实 transport 调用数、成功/失败、返回行数、总计与平均延迟、峰值在途数、60 秒窗口调用数，并按端点和错误类型分组；快照不包含 Token 或 URL。研究运行时按状态变化、最多每 5 秒一次以及每次采集结束的边界将这些指标合并投影到 `provider_health` 的 `kind=provider`、`provider_key=tushare`、`scope=account` 记录。健康投影失败只影响观测且会在下次重试，不改写已冻结数据或因子结果。

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `TUSHARE_GLOBAL_CALLS_PER_MINUTE` | `450` | 全账号共享的 60 秒滚动上限 |
| `TUSHARE_MAX_INFLIGHT` | `2` | 全账号物理在途请求上限 |
| `TUSHARE_ENDPOINT_LIMITS_JSON` | `{}` | 可选端点分钟上限，只能比总桶更严格 |

研究数据按不可变快照写入 `research_dataset_snapshots`，显式区分 `available`、`empty`、`partial`、`stale`、`permission_denied`、`not_supported` 和 `fetch_failed`。财务数据以公告日期确定 `available_at`，筹码数据按股票和交易日增量采集；任何晚于研究知识时点的数据都不会进入因子或快照。Value、Quality、Trend、Catalyst、Risk 因子完全由确定性代码计算，不调用 LLM，并区分缺失与不适用。

一旦本轮创建 `PreparedResearch`，`prepared.as_of` 就是后续上下文、Prompt 和 LLM 的唯一知识边界。Pipeline 只复用本轮冻结的名称、日线、估值、财务和筹码数据，并按该日期读取本地日线；当前实时报价、未版本化的搜索/社交/本地资讯、当前组合状态、板块排行和 AkShare 辅助补洞不会混入 Research Snapshot，缺失项保持缺失。外部文本在非研究旧路径中也只能作为不可信数据进入固定哨兵区，围栏、伪角色和内嵌指令会被转义且不能覆盖 system 指令。

冻结后的因子和实际消费数据分别写入 `research_factor_snapshots` 与 `research_snapshots`。`GET /api/v1/research/factors/{stock_code}`、`GET /api/v1/research/snapshots/{snapshot_hash}` 和 `GET /api/v1/research/datasets/{stock_code}` 仅用于查询；功能开关关闭后仍可读取历史。数据集查询默认只返回摘要，只有显式 `detail=true` 才返回标准化数据。

原始研究文件存放于 `data/research/raw/`，SQLite 在线备份不包含该目录。需要完整恢复能力时，必须按[研究原始数据归档与取证恢复](operations/research-raw-backup.md)把原始目录与对应 SQLite 备份成对归档；即使原始文件按保留策略清理，不可变 Research Snapshot 仍保留当次实际消费的标准化值。

`scripts/fetch_tushare_stock_list.py` 是离线管理员维护工具，保留历史 Tushare SDK 契约，不属于 Durable Worker 账号桶。生产执行前必须停止研究 Worker，并避免与在线采集并行；正常分析、筛选任务和 Scheduler 不得以该脚本绕过统一 Provider。

## 原计划 PR3：关注池、对账、预算与 Policy

增强关注池与持仓 union、Opening/Reconciliation 绝对状态回放及 Web 预览/应用详见[个人投研关注池与持仓对账](personal-research-watchlist-reconciliation.md)。Quick 50、Standard+Deep 20、Debate 8 的默认日预算，Decision Signal 正式字段和 Portfolio Policy Gate 见[个人投研任务、Skill、Debate 与 Thesis](personal-research-execution-artifacts.md)。

Gate 默认 `off`；`shadow` 只记录 would-block 不改动作，`enforce` 只会将被阻断的增风险动作降级为 `observe`，不阻断减仓/退出。无完整组合快照时保持缺失并 fail closed，不伪造零权重或零风险。

## Evidence/Claim 基础（原计划 PR4 组成）

启用 `RESEARCH_EVIDENCE_ENABLED=true` 后，Durable Worker 在已经持久化的因子快照和本轮冻结数据之上构造确定性 Evidence Snapshot。每份快照最多包含 32 条 claim 和 16 条 citation；claim 只使用 `factor_metric` / `reported_event`，citation 只引用已经持久化的 dataset / factor hash，并同时冻结 JSON Pointer、值哈希、可用时间、来源、标题、短摘录和可选规范 URL。构建和持久化边界会重新核对股票、市场、`as_of`、lineage、JSON Pointer 与值哈希；不合法或晚于知识边界的证据 fail closed，不会静默改成“支持”。

Evidence 阶段最多调用一次已注入的 SearchService 结果列表入口；这次高层调用保留既有 provider fallback，因此不承诺最多一次底层 provider 请求，但所有候选 provider 都禁止跟随结果 URL 抓正文。系统只把 bounded normalized 标题、snippet、来源、发布时间和经过校验的 `http` / `https` 规范 URL 写入 SQLite Dataset，`raw_ref=None`；本版不创建或持久化 result-page/raw sidecar。历史恢复只读取 durable job 已绑定的 `news_search` Dataset 与 Evidence Snapshot，不重新搜索。外部 snippet 继续按不可信内容隔离后才进入 Prompt。未来若引入搜索 raw sidecar，才适用搜索/新闻 90 天保留分类，并必须补齐可覆盖去重后重复引用的 occurrence / `last_observed` 边界。

Evidence 写入 `research_evidence_snapshots`，并以 `research_evidence_snapshot` JobEvent 绑定到每次实际消费它的 durable job；查询 job 时不依赖可能指向首次创建任务的 `origin_job_id`。`research_snapshots.evidence_snapshot_hash` 显式链接本轮消费的 Evidence hash。内容相同的重试或 lease reclaim 复用同一不可变行，但仍为实际消费任务写入绑定事件。

只读接口：

- `GET /api/v1/research/evidence`：至少提供 `job_id`、`research_snapshot_hash`、`stock_code` 之一；可选 `as_of` 必须带 UTC offset，`limit` 默认 20、最大 100。结果按 `as_of DESC, id DESC` 排序，`next_cursor` 是 opaque keyset cursor，客户端不得解析或自行构造。列表只返回 status、coverage、claim/citation count、hash 与 lineage 摘要，不返回完整 Evidence payload。
- `GET /api/v1/research/evidence/{evidence_hash}`：按 64 位小写 SHA-256 读取完整 typed claims/citations。管理员认证开启时，这两个接口与其它 `/api/v1/*` 接口一样需要有效 session Cookie。

关闭 `RESEARCH_EVIDENCE_ENABLED` 只停止新 Evidence 的采集、构建和写入；已有 Evidence、Research Snapshot 及其只读 API 仍可查询。Web 的 Run Flow 默认折叠“研究证据”，只有用户展开且当前来源是 Task 时才按 `taskId` 加载；先读分页摘要，再按 hash 加载展开项的 claim/citation。空结果使用中性状态，失败可重试，Task 切换会丢弃旧请求结果；来源链接仅在协议为 `http` / `https` 时渲染，并使用 `noopener noreferrer`。

## Bounded Research Debate 与可见性（原计划 PR4 组成）

启用 `RESEARCH_DEBATE_ENABLED=true` 还要求 Personal Research、Durable Jobs、Tushare Research、Factors 和 Evidence 全部开启。这个开关只授予能力，不代表全量任务执行 Debate；只有显式 `debate` 模式，或冻结材料达到冲突/重要性门槛，并且 Evidence 条件完整时才触发。详细门槛见[个人投研任务、Skill、Debate 与 Thesis](personal-research-execution-artifacts.md)。

触发后，Durable Worker 从同一份冻结 Evidence 顺序执行 Bull、Bear 两个独立、纯文本的高层 completion；每次 durable attempt 对每个尚未解析的 stance 至多调用一次，已经持久化的成功或终止失败 stance 不会重调。provider 返回到检查点提交之间的崩溃窗口仍可能让未持久化 stance 在 retry 中再次调用，因此不承诺整个 job 生命周期的物理请求 exactly-once。没有可引用 claim 时调用数为零。Debate 生成器无工具、网络或记忆入口，不检索新资料，也不直接产生 thesis、动作、目标价或仓位。每侧最多 6 条 argument 和 6 条 open question；每条 argument 最多引用 8 个既有 claim ID 和 8 个既有 citation ID，限制文本和置信度均有界。模型输出只是解释层，不能成为新 Evidence。

不可变的 exact request、单侧 turn 和最终 snapshot 分别持久化到 `research_debate_requests`、`research_debate_turns`、`research_debate_snapshots`；request hash 可作为 API lineage 摘要，但 exact messages 绝不通过 API 暴露。写入会重新校验 canonical payload/hash、Evidence lineage、Bull/Bear 顺序与计数，并受当前 lease、过期时间和取消状态 fence。单侧终止失败得到 `partial`，双侧终止失败得到 `generation_failed`；瞬时错误回到 durable retry。每个已确定的 stance 都在继续下一侧前先写入 lease-fenced 检查点：成功侧绑定 `research_debate_turn`，终止失败侧绑定仅含安全错误码的 `research_debate_failure`。因此 lease reclaim 只补同一 request hash 下尚未解析的 stance，prompt/route 漂移会 fail closed，不会重调已终止侧或拼接不同合同；完成后再绑定 `research_debate_snapshot`。`research_snapshots.debate_snapshot_hash` 固定本轮 Debate，且必须与 `evidence_snapshot_hash` 同链。

完整 Personal Research 的恢复边界同样是“检查点已持久化”，不是物理请求 exactly-once：已经提交的 Dataset/Debate/Research/Skill/Review/History/Signal/Thesis 会复用或 fail closed；provider/LLM response 到检查点 commit 的窗口仍可重调。主分析 LLM 以 Analysis History 为恢复检查点，History 一旦提交，terminal resume 必须在 Provider、Debate、主 LLM 之前命中，只补确定性的 Signal/Thesis 尾段。`SAVE_CONTEXT_SNAPSHOT=false` 也不移除 formal replay 所需的私有最小块；该块只含冻结 phase、Policy 模式/版本/hash/代码指纹与服务端组合输入，API 会剥离，尾段禁止重读当前 Portfolio 或当前 Policy 模式。

只读接口：

- `GET /api/v1/research/debates`：至少提供 `job_id`、`research_snapshot_hash`、`stock_code` 之一；可选 `evidence_snapshot_hash`、带 UTC offset 的 `as_of`、opaque `cursor` 和 `limit`（默认 20、最大 100）。结果按 `as_of DESC, id DESC` 稳定分页，只返回 status、Bull/Bear argument count、open-question count、hash 与 lineage 摘要，不返回完整 Debate 或 request messages。
- `GET /api/v1/research/debates/{debate_hash}`：按 64 位小写 SHA-256 读取严格 typed 的 bounded payload。`turns` 固定为 Bull/Bear 顺序；每项只包含 turn/prompt hash、实际模型、stance、summary、arguments 和 open questions，失败侧另以 `{stance,error_code}` 记录。

关闭 `RESEARCH_DEBATE_ENABLED` 只停止新 request/turn/snapshot 的构建和写入，历史 Debate 及其 API 仍可读取。Web Run Flow 在“研究证据”下方默认折叠“研究辩论”，仅对 Task 来源按需加载分页摘要和 hash 详情；列表、详情、加载更多均可重试，切换 Task 会丢弃旧响应，重复页按 Debate hash 去重。所有模型文本按纯文本渲染，不创建链接、不解释 HTML，也不使用浏览器原生 `title` 承载隐藏内容。

## 原计划 PR4：Skill、Verifier/Judge 与 Thesis

五项 Skill 在 LLM 前由冻结因子/Evidence 确定性计算，Debate 如实际触发则必须再通过 Verifier/Judge fail-closed 检查。`RESEARCH_THESIS_ENABLED=true` 后，系统在 Decision Signal 写入后追加不可变 Thesis；未触发 Debate 也可用 Evidence 形成 Thesis，因此 Thesis 不依赖 Debate 开关。

Skill execution 保存完整 Evidence Dataset lineage，并要求与 Evidence Snapshot 精确相等；Factor Snapshot 只保存其确定性计算实际消费的 Dataset，因此 Factor lineage 必须是该完整集合的子集，而不是被强制伪装为相等。例如 `news_search` 可被 Evidence 引用但不参与 Factor 计算。`2026-08-11-personal-research-skill-dataset-lineage` 追加迁移以事务方式替换旧插入触发器；失败会回滚触发器和 migration marker，既有不可变 Skill 行不被改写。

持久任务 API、任务模式、日预算、Skill 计算、Debate 触发、Verifier/Judge、Policy 门槛、Thesis/Artifact API 和 Web 缺失态见[个人投研任务、Skill、Debate 与 Thesis](personal-research-execution-artifacts.md)。

## 备份与恢复

生产切换前使用 [SQLite 在线备份、校验与恢复](operations/sqlite-backup.md) 创建带 SHA-256、Schema/Index hash、核心表计数、`quick_check` 和外键检查的备份对，再按[研究原始数据归档与取证恢复](operations/research-raw-backup.md)创建与该 SQLite 哈希及引用集合绑定的 raw 归档。迁移前 rollback 备份使用当前已部署版本的密封工具与旧 Schema 清单；迁移后必须改用候选版本完整默认清单再次备份、校验，不能把任一阶段的清单拿来冒充另一阶段。恢复演练必须执行 SQLite 严格校验与异名隔离恢复、raw 严格校验与隔离恢复，并通过 `RawArtifactStore` 抽样回读；记录数据库大小、raw 文件数与字节数、运行环境及总耗时，目标 RTO 不超过 15 分钟。默认核心表除任务、事件、Outbox、组件健康和不可变 Research Dataset / Factor / Evidence / Debate / Research Snapshot 外，还应覆盖关注池、Reconciliation、研究预算、Policy Evaluation、Skill Contract/Execution、Debate Review、Thesis 与 Decision Outcome v2 表；恢复演练不得只验证业务报告表。

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
| `DECISION_OUTCOME_V2_INTERVAL_MINUTES` | `60` | `1-1440`；仅唯一 Scheduler owner 使用 |
| `DECISION_OUTCOME_V2_BATCH_LIMIT` | `100` | `1-500`；每轮 durable job 候选上限 |
| `PORTFOLIO_POLICY_GATE_MODE` | `off` | `shadow`/`enforce` 还要求 Personal Research、Factors、Evidence |

`DECISION_OUTCOME_V2_ENABLED` 只控制个人投研 Outcome v2 的分阶段能力；现有 `decision-signal-v1` 后台维护由 `DECISION_SIGNAL_OUTCOME_ENABLED` 独立控制，默认开启，但只有通过 `SCHEDULE_ENABLED=true` 或 `--schedule` 启动的唯一 Scheduler owner 才会周期执行。关闭 v2 不会关闭 v1，反之亦然。v2 Scheduler 只按间隔和批量上限提交 durable job，Provider 访问只发生在 Worker；调度入队固定 `notify=false`。完整合同见[个人投研 Decision Outcome v2](decision-outcome-v2.md)。

建议生产启用顺序：

1. 先运行迁移检查和迁移应用。
2. 启用 `DURABLE_JOBS_ENABLED` 并验证旧任务契约。
3. 依次启用 Personal Research、Tushare Research、Factors、Evidence。
4. 先验证 Skill 与基础任务，再分别启用按需 Debate 和 Thesis；Debate 开关不得解读为全量运行。
5. Policy Gate 使用 `shadow` 完成至少 7 个交易日差异验收后，才可改为 `enforce`。
6. Outcome v2 只在 PR5 代码、路由、校准和生产验收独立完成后启用；不与 v1 互相代替。

任何不完整依赖组合都会在运行时配置加载阶段拒绝启动，并在 Web 配置保存阶段返回结构化校验错误。回滚时按相反顺序关闭开关；数据库迁移为追加式，关闭开关不要求降级数据库。
