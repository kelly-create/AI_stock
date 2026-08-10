# 个人投研 PR0–PR6 生产验收记录

本文是原始“个人 A 股投研二开与生产验证计划”PR0–PR6 的统一上线验收真源。它把代码完成、离线验证、生产部署、业务 canary、Policy Shadow 和最终 Enforce 分开记录；任何一项尚未发生时，不得用“代码已合并”或“接口返回 200”替代。

相关功能与字段合同见：

- [个人投研迁移与功能开关](personal-research-rollout.md)
- [个人投研关注池与持仓对账](personal-research-watchlist-reconciliation.md)
- [个人投研任务、Skill、Debate 与 Thesis](personal-research-execution-artifacts.md)
- [个人投研 Decision Outcome v2](decision-outcome-v2.md)
- [SQLite 在线备份、校验与恢复](operations/sqlite-backup.md)

## 验收状态定义

| 状态 | 含义 |
| --- | --- |
| `PASS` | 该门禁在本候选的固定字节或固定生产版本上已执行，证据可复核 |
| `BLOCKED` | 已发现必须先修复的正确性、安全性或阻断型 CI 问题 |
| `PENDING` | 尚未执行，或执行需要真实时间、凭据、Provider 权限或生产窗口 |
| `N/A` | 仅在合同明确不适用时使用，并写明理由；不得用来跳过失败项 |

只有 G0–G8 全部 `PASS`，才可称为“正常上线且 canary 验收完成”。只有 G9 的 7 个真实交易日 Shadow 也 `PASS`，才可评估把 `PORTFOLIO_POLICY_GATE_MODE` 从 `shadow` 改为 `enforce`。Outcome v2 在样本不足 30 条时应正确显示“样本不足”，不能伪造校准通过。

## 发布身份

每次候选和生产发布都必须填写下列固定身份；重新构建、重新迁移或重新生成静态规范后，旧记录不得沿用。

| 项目 | 记录值 |
| --- | --- |
| Git commit / tree | `PENDING` |
| Docker image digest | `PENDING` |
| Compose 与渲染后配置 SHA-256 | `PENDING` |
| SQLite 迁移 head | `PENDING` |
| 静态 OpenAPI SHA-256 | `PENDING` |
| Web build artifact SHA-256 | `PENDING` |
| 迁移前 rollback backup / manifest | `PENDING` |
| 迁移后 candidate backup / manifest | `PENDING` |
| raw archive / manifest | `PENDING` |
| 部署时间、执行人、验收人 | `PENDING` |

### 2026-08-10 未提交候选的离线证据

- 基线为 `94698b848f257b34fb5d2837a989b838ef82676f`；候选尚未提交，因此 Git commit、镜像和生产身份仍必须保持 `PENDING`。
- 三路独立语义终审已覆盖 PR3/PR4、Durable Resume、PR5/PR6、配置热更新、迁移顺序和备份核心表，当前未发现剩余 P0/P1。
- Web 最终字节基于既有、锁定版本的 `node_modules` 通过全量 ESLint、TypeScript、Vite production build；Vitest 为 110 个测试文件、1154 passed、2 skipped。当前环境没有 npm，尚未执行 clean `npm ci`，所以 G3 必须等 GitHub `web-gate` 后才能关闭。可视验收截图只存于仓库外证据目录，不作为仓库文件提交。
- 静态 OpenAPI 当前 SHA-256 为 `9f725255c5c9eebe68a4c4fb34ab64c75f6fb4b4aafd2ced538f9ff00ce1f6e4`；47 个 Web build 文件的规范化 tree SHA-256 为 `81a51c1c30e1b28b1ac8b4e603ea5bfdbf0d9bcd8204c9ffe4ba07d6777b9245`。Web tree 算法为：递归枚举 `static/` regular files，按 POSIX 相对路径排序，逐行写入 `<relative-path>  <file-sha256>`，使用 LF 与末尾换行拼接后再计算 SHA-256。
- 迁移测试为 31 passed、1 个 Windows 软链接能力跳过；候选版 SQLite 在线备份、严格校验、异名恢复演练得到 `quick_check=ok`、外键错误 0、55 张表。
- Windows 等价 syntax、critical flake8、确定性 code/yfinance 门已通过；完整 `pytest -m "not network"` 仍受 Windows/Unix 子进程、SQLite 文件锁和时钟粒度差异影响，不能替代 GitHub Linux `backend-gate`，所以 G2 在远端阻断型 CI 通过前仍为 `PENDING`。

## 门禁总表

| 门禁 | 验收范围 | 当前状态 | 必需证据 |
| --- | --- | --- | --- |
| G0 范围 | 原始 PR0–PR6 逐项映射，无编号漂移或隐藏排除项 | `PASS` | rollout 阶段表、CHANGELOG、配置依赖图 |
| G1 契约 | migrations、DB 约束、不可变 lineage、API/Pydantic、Web 类型、配置默认关闭 | `PASS` | 三路独立终审、静态 OpenAPI 与 runtime exact 对照、直接 SQL 反例 |
| G2 后端 | Python compile/lint、focused 回归、`ci_gate.sh`、非网络 pytest | `PENDING` | 命令、退出码、测试计数、日志或 CI URL |
| G3 Web | `npm ci`、全量 ESLint、Vitest、TypeScript、生产构建、关键页面视觉证据 | `PENDING` | 本地专项已 PASS；clean npm ci / GitHub web-gate 尚待执行 |
| G4 迁移 | 旧库检查、全量 apply、幂等 apply、失败注入回滚、约束/trigger 直写反例 | `PENDING` | 隔离旧库副本、migration markers、quick/FK/schema hash |
| G5 备份恢复 | 迁移前旧工具备份、迁移后候选工具全表备份、raw 配对、异名隔离恢复 | `PENDING` | 两阶段 manifest、所有核心表计数、RTO、恢复后抽样 |
| G6 镜像与编排 | Docker build/import smoke、normal/durable profiles、Worker heartbeat、唯一 Scheduler owner | `PENDING` | image labels/digest、Compose rendered diff、container health |
| G7 flag-off 部署 | 新 schema/image 上线但新增研究 flags 全关，旧 API/任务/通知无回归 | `PENDING` | readiness、旧合同、DB、容器身份、回滚演练 |
| G8 分阶段 canary | Durable → Tushare → Factors → Evidence → Skill → conditional Debate/Thesis → Outcome v2 | `PENDING` | 每阶段任务、JobEvent、Provider health、lineage、Web/API、Outbox |
| G9 Policy Shadow | 7 个真实交易日 shadow；差异、误阻断、缺失估值、账户动作和回滚均复核 | `PENDING` | 每日签字、样本与异常清单、最终 enforce 决策 |

## PR0–PR6 功能签收

| 阶段 | 必须证明的业务结果 | 离线 | 生产 |
| --- | --- | --- | --- |
| PR0 | 显式迁移、readiness、配置依赖、默认关闭、可恢复基线 | `PASS` | `PENDING` |
| PR1 | Durable Worker、lease/heartbeat、JobEvent、Outbox、唯一 Scheduler owner | `PASS` | `PENDING` |
| PR2 | Worker-only Tushare、冻结 Dataset/Factor、限流、raw 可恢复 | `PASS` | `PENDING` |
| PR3 | watchlist/legacy/holding union、Opening/Reconciliation、预算、Policy shadow/enforce | `PASS` | `PENDING` |
| PR4 | 五 Skill、模式/预算、条件 Debate、Verifier/Judge、immutable Thesis、durable resume | `PASS` | `PENDING` |
| PR5 | T+1、5/10/20d、复权、MFE/MAE、CSI300/SW1 点时基准、四维校准 | `PASS` | `PENDING` |
| PR6 | Web、OpenAPI、调度、通知、备份、部署与运维闭环 | `PENDING` | `PENDING` |

## Canary 与 Shadow 记录模板

每一行必须对应真实交易日和固定生产版本。任务未运行、Provider 权限不足、没有成熟 20d 样本或没有足够校准样本时，按真实原因记录，不得填零或跳过。

| 交易日 | 版本/镜像 | 开启阶段 | canary task/job | Provider 与 lineage | Policy shadow allow/would-block | Outcome mature/pending/unable | 异常与处置 | 复核人 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Day 1 | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` |
| Day 2 | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` |
| Day 3 | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` |
| Day 4 | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` |
| Day 5 | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` |
| Day 6 | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` |
| Day 7 | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` |

## Go / No-Go 规则

以下任一条件成立时必须 `NO-GO`：

- 阻断型 CI、全量 Web lint/build、迁移、备份恢复或 Docker smoke 非零退出；
- 新开关默认值不是关闭，或不完整依赖组合能启动；
- Durable retry 重新采集或重新调用已成功的 LLM 阶段，导致冻结 lineage 漂移；
- 正式 DecisionSignal、Skill、Review、Thesis、Outcome v2 或 Policy Evaluation 可被客户端伪造或终态覆盖；
- Outcome v2 使用当前 SW1 归属回填过去、使用自然日代替交易日，或把缺失基准写成零；
- Policy `enforce` 在 7 个真实交易日 shadow 验收前启用；
- 生产异常时无法证明旧镜像、旧 Compose、SQLite/raw 配对备份可以恢复；
- 验收探针在不可变目录内创建 WAL/SHM、临时文件或其它副作用。

## 回滚顺序

1. 停止继续启用新能力，并按逆序关闭 Outcome v2、Thesis/Debate、Evidence/Factors/Tushare、Personal Research、Durable Jobs；Policy Gate 先退到 `shadow`，再退到 `off`。
2. 保持 append-only 新表，不做在线降级迁移；若只是业务异常，优先回切旧镜像和旧 Compose。
3. 若 schema 或数据损坏，停写后使用迁移前 SQLite rollback backup 与对应 raw archive 做异名隔离恢复，严格校验后再交换规范路径。
4. 恢复旧 server/worker/analyzer 拓扑，验证 readiness、旧 API、Scheduler owner、DB quick/FK 和通知 Outbox，再恢复外部流量。
5. 在本文件记录触发原因、恢复锚点、RTO、丢失窗口和后续修复；失败 canary/checkpoint 不得复用或清理成成功证据。

## 当前结论

截至本候选开发阶段，PR0–PR6 的代码范围与离线专项验收已经冻结并通过独立复核，但 GitHub Linux 阻断型 CI 和生产 G4–G9 尚未完成，因此当前结论仍是：**不可宣称整个项目已经正常上线，也不可启用 Policy Enforce。** 后续每关闭一个门禁，都应把固定版本、命令、哈希和生产证据回填到本文件；仅更新叙述而不附可复核证据不构成验收。
