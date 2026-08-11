# 个人投研任务、Skill、Debate 与 Thesis

本文记录原始个人 A 股投研计划 PR3/PR4 已落地的运行契约：持久任务提交、研究模式与日预算、五项确定性 Skill、按需 Debate、Verifier/Judge、Portfolio Policy Gate 和不可变 Thesis。关注池与持仓对账另见[个人投研关注池与持仓对账](personal-research-watchlist-reconciliation.md)，数据、Evidence 与分阶段启用见[个人投研迁移与功能开关](personal-research-rollout.md)。

## 安全默认值

所有新增研究能力默认关闭，不配置时不改变旧分析、Decision Signal v1、持仓或通知路径：

| 配置 | 默认值 | 用途 |
| --- | --- | --- |
| `PERSONAL_RESEARCH_ENABLED` | `false` | 个人投研总开关 |
| `DURABLE_JOBS_ENABLED` | `false` | 只由 Durable Worker 执行研究任务 |
| `TUSHARE_RESEARCH_ENABLED` | `false` | 统一 Tushare 研究 Provider |
| `RESEARCH_FACTORS_ENABLED` | `false` | 确定性因子快照 |
| `RESEARCH_EVIDENCE_ENABLED` | `false` | typed Evidence/Claim 快照 |
| `RESEARCH_DEBATE_ENABLED` | `false` | 允许满足触发条件的任务进入 Debate |
| `RESEARCH_THESIS_ENABLED` | `false` | 在 Decision Signal 后生成终态 Thesis |
| `PORTFOLIO_POLICY_GATE_MODE` | `off` | `off` / `shadow` / `enforce` |

`RESEARCH_THESIS_ENABLED` 是独立开关。个人投研与 Evidence 已启用时，五项 Skill 执行可在 LLM 前持久化；只有 Thesis 开关也启用时，才会在 Decision Signal 写入后追加终态 Thesis。Thesis 不要求 Debate：未触发 Debate 的任务可以直接用 Evidence 生成 Thesis 材料。

## 提交持久个人投研任务

`POST /api/v1/research/personal/runs` 当前只接受单只 A 股。管理员认证开启时需有效 session Cookie，并且必须提供 8–128 位公开幂等标识 `Idempotency-Key`。一个成功请求返回 `202`；HTTP 202 只表示已入队，不代表研究成功。

PowerShell 示例：

```powershell
$body = @{
  stock_code = "600519"
  requested_mode = "auto"
  priority = 85
  manual_daily_override = $false
  notify = $true
  report_language = "zh"
} | ConvertTo-Json

curl.exe -X POST "http://127.0.0.1:8000/api/v1/research/personal/runs" `
  -H "Content-Type: application/json" `
  -H "Idempotency-Key: personal-600519-20260810-01" `
  --data-binary $body
```

响应包含 `task_id`、`trace_id`、`created`、`deduplicated`、`requested_mode`、`resolved_mode` 和 `priority`。使用 `GET /api/v1/analysis/status/{task_id}` 查询真实任务状态；验收时必须断言终态及业务产物，不能只断言提交返回 202。

启用任务接口的基础依赖是 Personal Research、Durable Jobs、Tushare Research、Factors 和 Evidence。请求或解析后的模式为 `debate` 时，还必须启用 `RESEARCH_DEBATE_ENABLED`。开关或 Tushare Token 不完整时返回 409，不会绕过 Worker 退回旧同步路径。

`manual_daily_override=true` 必须同时显式提交：

```json
{
  "manual_daily_override": true,
  "manual_daily_override_ack": "I_ACCEPT_DAILY_BUDGET_OVERRIDE"
}
```

这个 override 只越过当日数量预算，不越过 Durable lease、并发、取消、证据或 Policy 检查。

## 任务模式与日预算

`requested_mode` 支持 `auto`、`quick`、`standard`、`deep`、`debate`。显式模式保持不变；`auto` 按优先级确定性解析：

| priority | `resolved_mode` |
| --- | --- |
| 0–39 | `quick` |
| 40–79 | `standard` |
| 80–100 | `deep` |

日预算按冻结研究时间对应的市场本地日期、任务、市场、股票和 bucket 幂等保留；同一任务后续因冲突升级 Debate 时必须沿用首个 reservation 的日期，不能在午夜边界重复或跨日扣减：

| bucket | 默认上限 | 配置 |
| --- | --- | --- |
| Quick | 50 | `RESEARCH_QUICK_DAILY_BUDGET` |
| Standard + Deep | 20 | `RESEARCH_STANDARD_DEEP_DAILY_BUDGET` |
| Debate | 8 | `RESEARCH_DEBATE_DAILY_BUDGET` |

三个预算配置均接受 `0`，表示个人部署不限制每日次数。无限模式仍保留 Durable lease、任务去重、并发、取消、不可变血缘和 Policy 检查，只关闭按日数量拒绝；预算账本继续记录任务，便于审计与成本回看。

预算不足时任务 fail closed；重试复用同一任务保留，不应反复扣减。

## 五项确定性 Skill

Skill 不调用模型，不读取新网络数据，只消费同一份冻结 Research/Factor/Evidence lineage。缺失不会被当成 0：

| Skill ID | Decision Signal 字段 | 计算 |
| --- | --- | --- |
| `personal-value-quality` | `value_quality_score` | 冻结 Value 与 Quality 分数的算术平均 |
| `personal-trend-timing` | `trend_timing_score` | 冻结 Trend/Timing 分数原值 |
| `personal-catalyst` | `catalyst_score` | 冻结 Catalyst 分数原值 |
| `personal-risk` | `risk_score` | 冻结 Risk 分数原值，不反转 |
| `personal-evidence-quality` | `evidence_quality_score` | Evidence coverage × 100 |

每个执行显式保留 Skill ID、版本、contract hash、input/output hash、Research/Factor/Evidence hash 和 dataset lineage。五项 Skill 结果必须与 Thesis 中的 execution hash 一一对应。

## Debate 是按需触发，不是全量执行

`RESEARCH_DEBATE_ENABLED=true` 只打开 Debate 能力，不会把每个 Quick/Standard/Deep 任务升级为 Debate。实际触发需同时满足：

- 开关已启用；
- 存在可引用 Evidence；
- `evidence_quality_score` 不为空；
- 显式 `mode=debate`，或材料冲突分数至少 60，或重要性分数至少 80。

自动触发还要求 Evidence Quality 至少 70。显式 Debate 不套用这个 70 分自动门槛，但仍要求可引用 Evidence 且质量分不为空。未满足条件时保留稳定 reason code，不会静默强行运行 Debate。

Bull/Bear 只能引用已冻结 claim/citation。Prompt v2 与 Judge 共用 0.5 门槛：只有诚实置信度至少为 0.5 的证据支撑观点才进入 `arguments`，较弱候选观点保留在 `limitations` / `open_questions`，不得为通过门槛抬高置信度；如果某一侧没有任何观点诚实达到门槛，仍输出一条有界低置信度观点，让 Judge 明确 fail closed。每个已配置模型的实际请求同时携带严格 `json_object` 响应格式，返回后仍由完整 `research-debate-output-v1` 业务校验器检查 stance、字段和 Claim/Citation 引用；JSON mode 不能替代业务校验，也不会在模型不支持时降级为无约束成功。Verifier 确认快照完整、Bull/Bear 齐全、参数有引用且 Evidence lineage 匹配；Judge 在 Verifier 通过后计算两侧平均置信度。任意一侧平均置信度低于 0.5 时 fail closed；两侧差值绝对值小于 0.15 时为 `balanced`，否则返回 `bull` 或 `bear`。Verifier/Judge 都保留版本、输入/输出 hash 和 reason codes；任一 fail-closed review 不能支撑正式 Thesis。

## Portfolio Policy Gate

Gate 只处理已开始提供正式个人投研字段的 Decision Signal。Legacy writer 不会因全局设置为 `shadow`/`enforce` 而被追加新的必填要求。

| 模式 | 语义 |
| --- | --- |
| `off` | 默认。保留兼容路径 |
| `shadow` | 持久化 `would_block`、reason codes 和评估 hash，不修改提议动作 |
| `enforce` | 风险增加动作被阻时确定性降级为 `observe` |

默认 `personal-cn-v1` 限制为：Value/Quality ≥ 65、Trend/Timing ≥ 65、Evidence Quality ≥ 70、Risk ≤ 45；新建仓投影权重 ≤ 5%、加仓后常规上限 10%、单仓硬上限 15%、行业投影权重 ≤ 30%、单仓风险 ≤ 1%。风险增加动作缺失完整组合快照、投影权重、行业权重或单仓风险时 fail closed，不伪造 0。

`reduce_candidate` 和 `exit_candidate` 不被建仓限制阻断；Gate 不能让降风险动作变得更难。切换 `enforce` 前必须先完成 7 个交易日 `shadow` 观察并审核差异报告。

## Thesis 与只读 Artifact API

Thesis 是 append-only 终态产物，保留五项 Skill execution hash、Research Snapshot hash、可选 Debate/Review lineage、stance、account action、scores、catalysts、invalidators、unknowns 和 evidence refs。正式 Decision Signal 在 Policy 为 `off` 时也必须绑定，确保按 signal 查询可见；仅完整 Policy 评估及 Portfolio Snapshot 存在时，再额外绑定 Policy hash 和 Portfolio Snapshot ref。

历史 artifact 读取不受新写入开关影响：

- `GET /api/v1/research/personal/artifacts/skills/tasks/{task_id}/stocks/{market}/{stock_code}`
- `GET /api/v1/research/personal/artifacts/skills/{execution_hash}`
- `GET /api/v1/research/personal/artifacts/debate-reviews/{review_hash}`
- `GET /api/v1/research/personal/artifacts/theses/{thesis_hash}`
- `GET /api/v1/research/personal/artifacts/theses/by-signal/{decision_signal_id}`
- `GET /api/v1/research/personal/artifacts/theses/latest?task_id=...&market=...&stock_code=...`

Skill 集合响应显式返回 `expected_skill_ids`、`missing_skill_ids` 和 `complete`。未找到的 artifact 返回 404；客户端必须展示缺失态，不能用 0 补齐。`by-signal` 只能查到已完整绑定 Decision Signal/Policy/Portfolio lineage 的 Thesis；未绑定时使用 task/stock latest 或 Thesis hash 读取。

Decision Signal 同时保留 legacy `action` 和正式 `research_stance` / `account_action`、五项分数、Research/Policy/Portfolio hash、`policy_mode`、`policy_decision`、`would_block` 与 `policy_reasons`。这些字段是追加式兼容扩展，不将 legacy Decision Signal 重解释为正式 Thesis；通用 create API 不接受客户端伪造这些 server-owned 正式字段。

Portfolio Policy context 同时封存 `decision_session_date` 与 `valuation_bar_date`：前者绑定正式决策与行业事实，后者使用市场日历给出的最新已完成日线。周末、节假日和盘前不会仅因“上一收盘日早于自然日”而误判 stale；缺少该有效交易日行情时仍 fail closed。`open_candidate` 必须没有现有仓位，`add_candidate` 必须已有仓位且目标权重严格高于当前权重。

## Web 只读可见性

Decision Signal 主卡和详情在存在 `account_action` 时，以它作为主决策，并并列显示 Policy verdict；legacy `action` 仅作为“上游研究动作”解释。详情中的 `Personal Research Thesis` 区块只读展示五项 Skill 分数/版本/lineage、Verifier/Judge、Thesis 结论和证据引用，并显式与 legacy Skill Outcome / Decision Outcome v1 分开。`null` 或 404 显示“尚无正式 Thesis”，不伪造 0 分；Skill/Review 子资产读取失败时保留 Thesis 并显示降级警告。

用户可见页面改动在 PR 中应附当前页面截图；截图作为 PR 描述、评论或 Actions artifact 证据，不将一次性验收图片合入仓库。

## Decision Outcome v1 / v2 边界

现有 Decision Outcome v1 仍由 `DECISION_SIGNAL_OUTCOME_ENABLED` 维护，与个人投研的 `DECISION_OUTCOME_V2_ENABLED` 独立。Outcome v2 已使用独立路由、Schema、表、Web 面板和静态 OpenAPI 契约；它的启用仍须通过[个人投研 Decision Outcome v2](decision-outcome-v2.md)的迁移、Worker、Provider、Web 与生产 canary 验收。不得用 v1 结果假冒 v2 样本，也不得把尚未完成的 v2 生产验收写成已上线。

## 回滚

回滚时按 `enforce` → `shadow` → `off`、Thesis、Debate、Evidence、Factors、Tushare Research、Personal Research 的逆序关闭新写入，最后再评估 Durable Jobs。开关关闭不删除历史 artifact；迁移为追加式，不要通过手工删表或改 hash 伪造回滚。
