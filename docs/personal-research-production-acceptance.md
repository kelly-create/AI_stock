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
| `IN_PROGRESS` | 已进入生产验收且已有可复核证据，但仍缺少合同规定的正向 canary、真实交易日或人工签字 |
| `BLOCKED` | 已发现必须先修复的正确性、安全性或阻断型 CI 问题 |
| `PENDING` | 尚未执行，或执行需要真实时间、凭据、Provider 权限或生产窗口 |
| `N/A` | 仅在合同明确不适用时使用，并写明理由；不得用来跳过失败项 |

只有 G0–G8 全部 `PASS`，才可称为“正常上线且 canary 验收完成”。只有 G9 的 7 个真实交易日 Shadow 也 `PASS`，才可评估把 `PORTFOLIO_POLICY_GATE_MODE` 从 `shadow` 改为 `enforce`。Outcome v2 在样本不足 30 条时应正确显示“样本不足”，不能伪造校准通过。

## 发布身份

每次候选和生产发布都必须填写下列固定身份；重新构建、重新迁移或重新生成静态规范后，旧记录不得沿用。

| 项目 | 记录值 |
| --- | --- |
| Git production commit / tree | `93c98dfbabbba82ca686ed6df84040a13a9988c4` / `54fcbeecb8867498940d31439a22b95d99bc58c6` |
| Docker image digest | `sha256:0474a075f4876960cc90b4f405e91ccfae489ce893d22d27ff40c0f6970aec22`（OCI revision 为生产 commit） |
| Compose 与渲染后配置 SHA-256 | 当前 `canary-outcome-shadow-thesis.json`：`d645b4fc9c99de580acefaf2feafe9c83190ec418499c756f5bd7d8dc1b1be18` |
| SQLite 迁移 head | `2026-08-11-personal-research-skill-dataset-lineage`，共 11 条 marker |
| 静态 OpenAPI SHA-256 | `9f725255c5c9eebe68a4c4fb34ab64c75f6fb4b4aafd2ced538f9ff00ce1f6e4` |
| Web build artifact SHA-256 | 生产镜像内 47 文件规范化 tree：`68c0d693118a2d4b64c1bb76b568e8d9ff1efe9fb34a7ee520dd5229ab4e0de4`；`build-info.json` revision 为生产 commit |
| 迁移前 rollback backup / manifest | 初始切换：`cb60f5219a522f17ca7b60d7615647d0c98be5ef621fe803a127d0534c6a493d` / `ad02927fe88cfda4249d416d79f7deecb274c435d7709cc01599a4cc47c94bc1`；lineage 切换：`5edfbab9891eb801b9633f021d5278e9e1d14e53d11043fd27e6106894d9c67f` / `efc15172b235c8dd1e31ebab5f3aacf417bcb31ac59063c6c14ac3dc32a83c9d` |
| 迁移后 candidate backup / manifest | 初始切换：`a9ae9882f178bb06802c96006213b6ea491ffd07ebab51e7fad423cf2a1c60ed` / `833d3d15ba6264152ea0acc2dd856b9b558991e60c9dcff6778afd28fbfbcc31`；lineage 切换：`81da02352c50a1d1aafc4fddbc0a3650a8c88264b02d09c6fcf5baca1661cad9` / `c6d61ae8e48498bcb54ca92d50bc2691882443337db9acd59f415dfc07d86403` |
| raw archive / manifest | lineage 切换前后配对归档 SHA-256 均为 `4f60979080452e0eb2722ce6d3289433ae557fc01987d511ea6939d8773d7f85`；生产观察基线另验证当前禁止路径不存在 |
| 部署时间、执行人、验收人 | 初始切换 `2026-08-10T18:24:47Z`；最终代码切换 `2026-08-11T05:03Z`；自动验收已完成，7 日 Shadow 与人工签字进行中 |

### 2026-08-11 PR #4 候选的离线与 CI 证据

- 基线为 `94698b848f257b34fb5d2837a989b838ef82676f`；功能候选提交为 `abcd935b9ab9b6b4c8af15e10d011033ac237acd`，tree 为 `0ffeeb2f4fbfe10a82bdeb28c06881b3786a3361`。本节的验收状态回填属于 docs-only 收口；最终 PR head 与生产身份仍须以 PR #4 当前 Head CI、合并提交和密封镜像记录为准。
- GitHub Actions [CI run 31404437702](https://github.com/kelly-create/AI_stock/actions/runs/31404437702) 对功能候选完成并通过：`ai-governance`、三片 `backend-tests`、汇总 `backend-gate`、`docker-build`（build、smoke、imports）和 `web-gate` 全部 `success`；Windows/macOS desktop-futu-package 因条件不适用而 `skipped`，不是失败。
- 三路独立语义终审已覆盖 PR3/PR4、Durable Resume、PR5/PR6、配置热更新、迁移顺序和备份核心表，当前未发现剩余 P0/P1。
- Web 最终字节在本地基于锁定依赖通过全量 ESLint、TypeScript、Vite production build；Vitest 为 110 个测试文件、1154 passed、2 skipped。GitHub `web-gate` 又从 clean `npm ci` 开始完成 lint 与 production build；三张页面验收截图已作为 [PR #4 评论附件](https://github.com/kelly-create/AI_stock/pull/4#issuecomment-5243134319) 发布，未作为仓库文件提交。
- 静态 OpenAPI 当前 SHA-256 为 `9f725255c5c9eebe68a4c4fb34ab64c75f6fb4b4aafd2ced538f9ff00ce1f6e4`；47 个 Web build 文件的规范化 tree SHA-256 为 `81a51c1c30e1b28b1ac8b4e603ea5bfdbf0d9bcd8204c9ffe4ba07d6777b9245`。Web tree 算法为：递归枚举 `static/` regular files，按 POSIX 相对路径排序，逐行写入 `<relative-path>  <file-sha256>`，使用 LF 与末尾换行拼接后再计算 SHA-256。
- 迁移测试为 31 passed、1 个 Windows 软链接能力跳过；候选版 SQLite 在线备份、严格校验、异名恢复演练得到 `quick_check=ok`、外键错误 0、55 张表。
- Windows 等价 syntax、critical flake8、确定性 code/yfinance 门已通过；完整本地 `pytest -m "not network"` 受 Windows/Unix 子进程、SQLite 文件锁和时钟粒度差异影响。权威 Linux CI 已通过三片离线测试与汇总 `backend-gate`，因此该本地平台差异不再阻断 G2。

### 2026-08-10 至 2026-08-11 生产切换与 canary 证据

- 初始四迁移、flag-off 切换及两阶段备份证据位于 `/opt/dsa-backups/personal-cutover-20260810T182351Z-05bdac1-retry2`；结果固定 10 条迁移、53 张表、Policy `off`，并保留迁移前后 SQLite/manifest 哈希。
- 新增 Skill Dataset lineage 迁移先在 `/opt/dsa-backups/personal-lineage-migration-rehearsal-20260811T032100Z-76d4df3b` 隔离演练，再由 `/opt/dsa-backups/personal-lineage-cutover-20260811T033527Z-76d4df3b` 完成生产迁移、幂等复核、迁移前后 SQLite/raw 配对归档与服务恢复；当前迁移 head 为 11 条。
- 最终代码身份由 `/opt/dsa-backups/personal-code-cutover-20260811T050253Z-93c98dfb` 固定；当前 server、analyzer、worker 均运行同一镜像 ID，searxng 保持原容器，全部 healthy、restart 0。
- Thesis 正向验收为 `/opt/dsa-backups/personal-thesis-acceptance-20260811T053500Z-93c98dfb`；Policy Shadow 正向验收为 `/opt/dsa-backups/personal-shadow-canary-20260811T060622Z-93c98dfb`，对应 signal `122`、完整 Portfolio context、`would_block=false`。
- Outcome v2 正向提交与 pending 生命周期验收为 `/opt/dsa-backups/personal-outcome-shadow-promotion-20260811T062832Z-93c98dfb`：6 个 signal×horizon 候选均保持 `pending/entry_session_not_reached`，且 observation hash、lineage 与不可变状态完整。
- Shadow Day 1 基线位于 `/opt/dsa-backups/personal-shadow-observation-acceptance-20260811T063353Z-93c98dfb`，固定 7 个 XSHG 交易日为 `2026-08-11/12/13/14/17/18/19`；自动门已通过，人工签字待完成。
- Debate 保持关闭。任务 `a2716ca555ad4ed7be41f60f311cb963`（600519）和 `4003b08a028345b4981c963c2dde8d07`（601985）均完成 20 份 Dataset、Factor、Evidence、双 stance 与 Verifier，随后被 Judge 以 `stance_confidence_below_minimum` 正确 fail-closed；对应 `.INCOMPLETE` 证据保留，Compose 已自动恢复。该安全负例通过，但在出现一次不放宽阈值的正向 Review/Thesis 前，G8 仍为 `IN_PROGRESS`。

## 门禁总表

| 门禁 | 验收范围 | 当前状态 | 必需证据 |
| --- | --- | --- | --- |
| G0 范围 | 原始 PR0–PR6 逐项映射，无编号漂移或隐藏排除项 | `PASS` | rollout 阶段表、CHANGELOG、配置依赖图 |
| G1 契约 | migrations、DB 约束、不可变 lineage、API/Pydantic、Web 类型、配置默认关闭 | `PASS` | 三路独立终审、静态 OpenAPI 与 runtime exact 对照、直接 SQL 反例 |
| G2 后端 | Python compile/lint、focused 回归、`ci_gate.sh`、非网络 pytest | `PASS` | 本地专项证据；GitHub CI run 31404437702 三片 backend-tests 与 backend-gate |
| G3 Web | `npm ci`、全量 ESLint、Vitest、TypeScript、生产构建、关键页面视觉证据 | `PASS` | 本地 1154 passed/2 skipped、ESLint/TypeScript/build；GitHub clean install/web-gate；PR 评论三图 |
| G4 迁移 | 旧库检查、全量 apply、幂等 apply、失败注入回滚、约束/trigger 直写反例 | `PASS` | 隔离演练与两次生产 migration evidence；11 markers、quick/FK 通过 |
| G5 备份恢复 | 迁移前旧工具备份、迁移后候选工具全表备份、raw 配对、异名隔离恢复 | `PASS` | 初始与 lineage 两阶段 SQLite/manifest/raw 配对证据及隔离恢复 |
| G6 镜像与编排 | Docker build/import smoke、normal/durable profiles、Worker heartbeat、唯一 Scheduler owner | `PASS` | 固定 image/OCI revision/Compose；server 抑制调度，analyzer 为唯一 owner，worker healthy |
| G7 flag-off 部署 | 新 schema/image 上线但新增研究 flags 全关，旧 API/任务/通知无回归 | `PASS` | flag-off 首次上线、readiness/API/DB/容器身份及逐阶段可逆切换证据 |
| G8 分阶段 canary | Durable → Tushare → Factors → Evidence → Skill → conditional Debate/Thesis → Outcome v2 | `IN_PROGRESS` | 除 Debate 正向 Review 外均已有成功任务；两次 Debate 安全负例正确 fail-closed |
| G9 Policy Shadow | 7 个真实交易日 shadow；差异、误阻断、缺失估值、账户动作和回滚均复核 | `IN_PROGRESS` | Day 1 自动基线已封存；2026-08-12 至 19 仍需逐日证据与人工签字 |

## PR0–PR6 功能签收

| 阶段 | 必须证明的业务结果 | 离线 | 生产 |
| --- | --- | --- | --- |
| PR0 | 显式迁移、readiness、配置依赖、默认关闭、可恢复基线 | `PASS` | `PASS` |
| PR1 | Durable Worker、lease/heartbeat、JobEvent、Outbox、唯一 Scheduler owner | `PASS` | `PASS` |
| PR2 | Worker-only Tushare、冻结 Dataset/Factor、限流、raw 可恢复 | `PASS` | `PASS` |
| PR3 | watchlist/legacy/holding union、Opening/Reconciliation、预算、Policy shadow/enforce | `PASS` | `IN_PROGRESS`（Shadow Day 1/7；Enforce 禁止） |
| PR4 | 五 Skill、模式/预算、条件 Debate、Verifier/Judge、immutable Thesis、durable resume | `PASS` | `IN_PROGRESS`（Skill/Thesis 已通过；Debate 正向验收待完成） |
| PR5 | T+1、5/10/20d、复权、MFE/MAE、CSI300/SW1 点时基准、四维校准 | `PASS` | `IN_PROGRESS`（6 个 Outcome 已正确 pending；成熟窗口与 n≥30 尚未到达） |
| PR6 | Web、OpenAPI、调度、通知、备份、部署与运维闭环 | `PASS` | `IN_PROGRESS`（G8/G9 尚未关闭） |

## Canary 与 Shadow 记录模板

每一行必须对应真实交易日和固定生产版本。任务未运行、Provider 权限不足、没有成熟 20d 样本或没有足够校准样本时，按真实原因记录，不得填零或跳过。

| 交易日 | 版本/镜像 | 开启阶段 | canary task/job | Provider 与 lineage | Policy shadow allow/would-block | Outcome mature/pending/unable | 异常与处置 | 复核人 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-08-11（Day 1） | `93c98dfb` / `0474a075…aec22` | Evidence、Skills、Thesis、Outcome v2、Policy Shadow；Debate off | Shadow `1761a658…ac1fc`；Outcome `fdb0dcef…5506e` | Dataset/Factor/Evidence/Skill/Thesis lineage 通过；Outcome historical replay 无 Provider 漂移 | `no_action` / `would_block=false`，Portfolio context 完整 | 6 pending，均为 `entry_session_not_reached`，没有伪造 0 | 两次 Debate Judge 低置信度 fail-closed；生产自动恢复且健康 | 自动门 PASS；人工待签 |
| 2026-08-12（Day 2） | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` |
| 2026-08-13（Day 3） | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` |
| 2026-08-14（Day 4） | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` |
| 2026-08-17（Day 5） | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` |
| 2026-08-18（Day 6） | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` |
| 2026-08-19（Day 7） | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` | `PENDING` |

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
2. 保持 append-only 新表，不做在线降级迁移。迁移后数据库包含旧版本未知 marker，因此不得只回切旧镜像；如需回到 PR4，必须停写并同时恢复匹配的迁移前 SQLite/raw 备份与旧 Compose。
3. 若 schema 或数据损坏，停写后使用迁移前 SQLite rollback backup 与对应 raw archive 做异名隔离恢复，严格校验后再交换规范路径。
4. 恢复旧 server/worker/analyzer 拓扑，验证 readiness、旧 API、Scheduler owner、DB quick/FK 和通知 Outbox，再恢复外部流量。
5. 在本文件记录触发原因、恢复锚点、RTO、丢失窗口和后续修复；失败 canary/checkpoint 不得复用或清理成成功证据。

## 当前结论

截至 2026-08-11，固定生产版本已经完成 G4–G7 并运行于服务器；Evidence、Skills、Thesis、Outcome v2 与 Policy Shadow 的生产 canary 已通过。G8 仍缺一次不放宽阈值的 Debate 正向 Review，G9 处于 Day 1/7，因此当前准确结论是：**系统已受控上线，但尚不可宣称全部 canary 与七交易日验收完成，也不可启用 Policy Enforce。** 后续每关闭一个门禁，都必须继续回填固定版本、哈希和生产证据；仅更新叙述而不附可复核证据不构成验收。
