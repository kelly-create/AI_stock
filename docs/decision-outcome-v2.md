# 个人投研 Decision Outcome v2

Decision Outcome v2 是个人投研链路的独立、不可变后验评估合同。它不替代 `decision-signal-v1`，也不复用 v1 的结果表、统计口径或 Web 卡片。v1 继续服务通用 DecisionSignal；v2 只评估带完整 Research Snapshot、Policy Evaluation 与 Portfolio lineage 的个人投研信号。

## 评估口径

- 执行价固定取信号会话后的下一可交易上交所会话（T+1）的未复权开盘价；停牌、涨跌停或缺少必要数据时写入明确状态和 `reason_code`，不猜测成交。
- 观察周期固定为 `5d`、`10d`、`20d` 个交易日。每个 `(signal_id, horizon, engine_version)` 是唯一、幂等且终态不可变的结果身份。
- 收益使用冻结的日线和复权因子计算，同时保存方向收益、MFE 和 MAE。缺失值保持 `null`，API 与 Web 都不得补成 `0`。
- CSI 300 使用 `000300.SH`；申万一级行业按信号日的点时成员关系解析。基准或行业不可用时分别保存 `status=unavailable` 与原因，不阻断标的自身可评估结果。
- `long` 与 `defensive` 采用相反方向解释；`observational` 仅记录观察结果，不伪造命中/未命中。
- `signal_status` 在每个结果身份首次创建 pending 行时冻结。来源 DecisionSignal 在 5/10/20 交易日观察成熟前正常从 `active` 过期，不会使该 pending 行失效；后续推进仍必须保持最初冻结的状态值，且除这一正常生命周期字段外，Signal、Policy 与 Dataset lineage 继续逐项 fail-closed 校验。

## API 合同

所有路由都在 `/api/v1/decision-signals` 下，并沿用可选管理员认证；启用认证时需要 `AdminSessionCookie`。

| 方法与路径 | 行为 |
| --- | --- |
| `POST /outcomes-v2/run` | 返回 `202`，只提交 durable job。要求 8–128 字符的 `Idempotency-Key`；body 支持 `signal_id`、唯一 `horizons`、A 股 `stock_code`、`decision_profile`、`limit` 与默认 `false` 的 `notify`，不提供 `force` 绕过。 |
| `GET /outcomes-v2` | 分页查询独立 v2 集合，可按信号、周期、引擎、状态、动作族、profile 和股票过滤。 |
| `GET /outcomes-v2/stats` | 查询 engine/horizon/profile/action-family 四维精确分桶。重复 `horizons` query 参数表示多个周期。 |
| `GET /{signal_id}/outcomes-v2` | 查询单个 DecisionSignal 的 v2 结果；信号不存在时返回 `404`。 |

POST 仅在 `DURABLE_JOBS_ENABLED`、`PERSONAL_RESEARCH_ENABLED`、`TUSHARE_RESEARCH_ENABLED`、`RESEARCH_FACTORS_ENABLED` 与 `DECISION_OUTCOME_V2_ENABLED` 全部开启时接收入队，否则返回 `409`。GET 不受功能开关限制，关闭功能后仍可审计已保存历史。

## 校准统计

统计分桶维度严格为 `engine + horizon + profile + final_action_family`，不跨分桶合并样本。每个分桶至少需要 30 个含置信度的已评估方向样本才返回 `accuracy`、ECE 和 Brier 分数；CSI 300 与申万一级行业的超额统计也各自独立要求 30 个有效样本。

不足 30 个样本时仍返回总数、有效数和固定 5 个置信度 bin，但推断指标为 `null`。Web 显示 `n/30` 与“样本不足”，绝不把 `null` 显示成 `0`，也不把 v1 样本计入 v2。

## 调度与通知

| 配置 | 默认值 | 约束 | 说明 |
| --- | --- | --- | --- |
| `DECISION_OUTCOME_V2_ENABLED` | `false` | boolean | v2 分阶段总开关；不影响 v1。 |
| `DECISION_OUTCOME_V2_INTERVAL_MINUTES` | `60` | `1-1440` | 唯一 Scheduler owner 两轮 v2 入队之间的间隔。 |
| `DECISION_OUTCOME_V2_BATCH_LIMIT` | `100` | `1-500` | 每轮 durable job 的候选上限。 |

Scheduler 只入队，不在本地调用 Tushare 或执行评估；默认 `notify=false`。人工 POST 可显式设置 `notify=true`，但通知只使用低敏公开摘要，不包含 Portfolio Snapshot、Policy 原始上下文、Dataset hash 列表、token、Cookie 或 webhook 地址。单一通知渠道失败不得改变已经持久化的评估终态。

## Web 展示

“AI 建议”页有独立的“个人投研 Outcome v2”面板：

- 最近结果逐条显示 T+1 可执行性、5/10/20d、方向收益、MFE/MAE、CSI 300/申万一级行业方向超额以及原因码。
- 校准卡按完整四维 bucket 展示 `n/30`、accuracy、ECE、Brier 与两个基准统计。
- v2 面板不读 v1 endpoint，不把两种结果相加；v2 请求失败不阻断现有 v1 信号列表与统计。

涉及该页面的 PR 描述必须附页面截图；应至少覆盖“样本不足且指标为不可用”与“已评估结果”之一，无法连接测试数据时可使用受控 fixture 截图并说明数据来源。

## 验收与回滚

上线前至少确认：

1. migration check/apply 成功，SQLite `quick_check`、外键检查及 Outcome v2 不可变触发器通过。
2. POST 只产生 `decision_outcomes_v2` durable job，Scheduler owner 唯一，Worker heartbeat/lease 正常，Scheduler 进程没有 Provider 调用。
3. 使用冻结 fixture 验证 T+1、停牌/涨跌停、5/10/20d、MFE/MAE、CSI 300、点时申万一级行业和所有失败原因。
4. 统计明确验证 29 个样本仍为 `null`、30 个样本才产生校准指标；Web 不出现伪造 `0`。
5. 静态 OpenAPI 与 runtime 完全一致，Web 定向测试、TypeScript、构建和受影响 eslint 通过。

`2026-08-11-personal-research-v2-signal-status-lineage` 追加迁移只替换 Outcome v2 的 insert/update lineage 触发器：insert 仍要求当前信号状态与冻结值一致，pending update 则保留首次冻结状态并继续验证其余 lineage。迁移失败会同时回滚触发器和 marker；不会改写、删除或回填既有 Outcome 行。

软回滚先设置 `DECISION_OUTCOME_V2_ENABLED=false`，停止新的 API/Scheduler 入队，但保留 GET 历史审计和已落库终态。随后等待运行中 job 到达安全终态，再回滚应用镜像。不要删除 v2 表、结果、Dataset 或 Portfolio/Policy lineage；需要恢复数据库时按[个人投研迁移与功能开关](personal-research-rollout.md)中的生产备份流程执行。
