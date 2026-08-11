# 个人投研关注池与持仓对账

本文说明原始个人投研 PR3 中的增强关注池、持仓期初和对账边界。这些能力不把持仓伪造成交易，也不会把一次预览当成已应用事件。研究任务、预算、Policy Gate 和 Thesis 见[个人投研任务、Skill、Debate 与 Thesis](personal-research-execution-artifacts.md)；完整阶段状态和默认关闭的生产开关见[个人投研迁移与功能开关](personal-research-rollout.md)。

## 有效研究集合

`GET /api/v1/research/universe` 按 `(market, stock_code)` 合并三个独立来源：

| 来源 | `sources[]` | 语义 |
| --- | --- | --- |
| 增强关注项 | `enhanced` | 包含原因、优先级、研究层级和下次复核时间 |
| 旧 `STOCK_LIST` | `legacy` | 保留现有配置、CLI 和兼容 API 语义 |
| Portfolio 账本持仓 | `holding` | 由不估值、不写缓存的纯账本 replay 实时计算 |

合并规则：

- 显式 active 增强项覆盖同一 identity 的默认 metadata。
- inactive 增强项是 tombstone，会压制同 identity 的 legacy 成员身份。
- 真实持仓始终保留；tombstone 只能移除关注来源，不能隐藏持仓。
- Settings 直接修改 `STOCK_LIST` 只改变 legacy source，不隐式删除 explicit enhanced source；已存在的 tombstone 仍压制后续 legacy resurrection。
- `holdings_freshness=ledger` 表示持仓来自当前账本 replay，不是 `portfolio_positions` 派生缓存。

市场 identity 优先使用显式 `market`。裸码持仓会被市场感知地规范化：港股补为 `HK` 五位码，日股默认 `.T`，韩股默认 `.KS`（可显式传 `.KQ`），台股默认 `.TW`（可显式传 `.TWO`）。

## 关注池 API

- `GET /api/v1/research/watchlist`：读取增强 metadata；`include_inactive=true` 包含 tombstone。
- `GET /api/v1/research/universe`：读取三来源 union；可传 `as_of=YYYY-MM-DD`。
- `PUT /api/v1/research/watchlist/{market}/{stock_code}`：完整替换 metadata。`reason`、`priority`、`analysis_tier`、`next_review_at` 四个字段必须全部提交，PUT 始终表示 active。
- `DELETE /api/v1/research/watchlist/{market}/{stock_code}`：写入 inactive tombstone 并移除 legacy 成员身份，成功返回 `{ "deleted": 1 }`。

`.env` 与 SQLite 是两个独立数据边界，不宣称跨存储原子提交。PUT 先写 enhanced，DELETE 先写 tombstone，再用同一次读取得到的 `config_version` 更新 `STOCK_LIST`。版本冲突返回 `409 config_conflict`；客户端应刷新后重试，重试不会丢失已写入的显式 metadata/tombstone。

`next_review_at` 以 UTC 保存，API 以带 `Z` 的 ISO-8601 字符串返回；排序依次是已到期、复核时间、优先级降序和稳定 identity。

## Portfolio 期初与对账

客户端只提交券商的绝对目标状态，不提交 delta：

1. `POST /api/v1/portfolio/accounts/{id}/reconciliations/preview`
2. 检查 `diff`、`warnings`、`book_hash` 和过期时间
3. `POST /api/v1/portfolio/accounts/{id}/reconciliations/apply`，只提交 opaque `preview_token` 与该 preview 固定的 `idempotency_key`

preview token 明文只返回给当前客户端，数据库只存 SHA-256。预览默认 15 分钟过期，过期状态会持久化为 `expired`。apply 在同一 `BEGIN IMMEDIATE` 事务中重算账本 hash、目标和 diff；账本漂移返回 `409 stale_preview`，相同 token + idempotency key 的重试返回同一 applied 事件。

回放顺序在同一天内固定为：

1. opening absolute baseline
2. cash ledger
3. corporate action
4. trade
5. reconciliation absolute state-set

同日多个 reconciliation 依实际 apply 生成的 `event_version` 排序，不依 preview ID。opening 每账户只允许一个 applied 事件，且必须不晚于首个账本活动日。reconciliation 不计作交易、费用、税费或 realized PnL；持仓 target 会成为新的绝对基线并将 FIFO lots 折叠为一个带 lineage 的 baseline lot。即使聚合数值未变，这个 FIFO 重置仍是可观测副作用，preview/detail 会返回 `fifo_lineage_reset:*` warning。

replay 从 header 中密封的完整 absolute target 重建现金和持仓，不只应用有变化的 adjustment rows。因此后续补录的更早账本事件不会穿透已应用的绝对边界。adjustment rows 仅作为不可变 before/after 审计证据；detail API 同时返回 `source`、canonical `target`、`warnings` 和 adjustments，不返回 preview token。

## Web 交互

- 首页列表、历史查询和批量分析使用 effective universe；legacy toggle 保持兼容。
- `sources === ['holding']` 的行不显示删除操作；mixed source 删除关注后仍保留 holding 行。
- Portfolio 页先预览 absolute cash/position target，再显式点击应用。响应丢失后重试会复用同一 preview-scoped idempotency key。

## 迁移与回滚

生产启用前先执行：

```bash
python -m src.migrations --check
python -m src.migrations --apply
```

迁移只追加表、索引和不可变性 trigger。如需回滚应用版本，先恢复已验证的 SQLite 备份和旧镜像；不要通过修改/删除 applied header 或 adjustment rows 伪造回滚。
