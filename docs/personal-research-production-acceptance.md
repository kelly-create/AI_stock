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
| Git production commit / tree | `f1aeb22bca593fefb88e12bd3c03deb2e7cdbfbc` / `2cf4d02dd399a072b95b93cbf1c5cc670f582f39` |
| Docker image digest | `sha256:fa7e712bc84195e48b8d6c36507bf1d7cb84faf35345bfb27a96b0bf1aa16abe`（OCI revision 为生产 commit） |
| Compose 与渲染后配置 SHA-256 | 当前 `canary-outcome-shadow-thesis-statuslineage.json`：`6fe8a4dab606d8b22d0a890bf5ba990d96dc93a30accfa4101b078458789c6d7` |
| SQLite 迁移 head | `2026-08-11-personal-research-v2-signal-status-lineage`，共 12 条 marker |
| 静态 OpenAPI SHA-256 | `9f725255c5c9eebe68a4c4fb34ab64c75f6fb4b4aafd2ced538f9ff00ce1f6e4` |
| Web build artifact SHA-256 | 生产镜像内 47 文件规范化 tree：`ef36e54f8566bd88684ab62b8073f3cbac1d6f34a9ccb01560959a653677ae26`；`build-info.json` revision 为生产 commit |
| 迁移前 rollback backup / manifest | Outcome status-lineage 切换：`88e90638e9f9009b377a6fdbf5eebd9dc8019916390165cce10aba27eee2dae6` / `1e1df14adac2245550c1b5f0dedb46bf81489db0ff990ac848a377483903210b` |
| 迁移后 candidate backup / manifest | Outcome status-lineage 切换：`a0aed7c8e83330ec9c4bf12a4cde2c49459d9c2d8e09e7caa9f8e9e2b0ff97d3` / `ec1219dff198563578700f14e30730653b39751616c5587463ade5d4421a02ed` |
| raw archive / manifest | 切换前后 raw 配对归档 SHA-256 均为 `b07726463f84708779cc42ac1526ea637ab695d0216890cf4d4c628ab7379616`；迁移前 manifest 为 `00dee060c1c82f84905f970ad2507d0cc8c3d4f5071c2980ef2db438886a51e8`，迁移后为 `bc170ea54c68e0132c3ac23faa6d8a7c16e11cf82ff4869445f8774373bb33c2`；170 个引用与文件全部闭合 |
| 部署时间、执行人、验收人 | 初始切换 `2026-08-10T18:24:47Z`；最终 Outcome status-lineage 切换 `2026-08-11T10:34:47Z`；自动验收已完成，7 日 Shadow 与人工签字进行中 |

### 2026-08-11 PR #4 / PR #5 候选的离线与 CI 证据

- 基线为 `94698b848f257b34fb5d2837a989b838ef82676f`；PR #4 功能候选提交为 `abcd935b9ab9b6b4c8af15e10d011033ac237acd`，tree 为 `0ffeeb2f4fbfe10a82bdeb28c06881b3786a3361`。后续 Prompt/Judge 与 Outcome status-lineage 修复在 Draft PR #5 收口，当前生产身份以本文件的 f1 提交、CI 和密封镜像记录为准。
- GitHub Actions [CI run 31404437702](https://github.com/kelly-create/AI_stock/actions/runs/31404437702) 对功能候选完成并通过：`ai-governance`、三片 `backend-tests`、汇总 `backend-gate`、`docker-build`（build、smoke、imports）和 `web-gate` 全部 `success`；Windows/macOS desktop-futu-package 因条件不适用而 `skipped`，不是失败。
- Outcome status-lineage 修复提交 `f1aeb22bca593fefb88e12bd3c03deb2e7cdbfbc` 的 [CI run 31480412332](https://github.com/kelly-create/AI_stock/actions/runs/31480412332) 也全部通过：三片后端测试、`backend-gate`、Docker build/smoke/import 与 `ai-governance` 均为 `success`；该提交未改 Web，因此 Web/desktop 条件性跳过。
- 三路独立语义终审已覆盖 PR3/PR4、Durable Resume、PR5/PR6、配置热更新、迁移顺序和备份核心表，当前未发现剩余 P0/P1。
- Web 最终字节在本地基于锁定依赖通过全量 ESLint、TypeScript、Vite production build；Vitest 为 110 个测试文件、1154 passed、2 skipped。GitHub `web-gate` 又从 clean `npm ci` 开始完成 lint 与 production build；三张页面验收截图已作为 [PR #4 评论附件](https://github.com/kelly-create/AI_stock/pull/4#issuecomment-5243134319) 发布，未作为仓库文件提交。
- 静态 OpenAPI 当前 SHA-256 为 `9f725255c5c9eebe68a4c4fb34ab64c75f6fb4b4aafd2ced538f9ff00ce1f6e4`；47 个 Web build 文件的规范化 tree SHA-256 为 `81a51c1c30e1b28b1ac8b4e603ea5bfdbf0d9bcd8204c9ffe4ba07d6777b9245`。Web tree 算法为：递归枚举 `static/` regular files，按 POSIX 相对路径排序，逐行写入 `<relative-path>  <file-sha256>`，使用 LF 与末尾换行拼接后再计算 SHA-256。
- 迁移测试为 31 passed、1 个 Windows 软链接能力跳过；候选版 SQLite 在线备份、严格校验、异名恢复演练得到 `quick_check=ok`、外键错误 0、55 张表。
- Windows 等价 syntax、critical flake8、确定性 code/yfinance 门已通过；完整本地 `pytest -m "not network"` 受 Windows/Unix 子进程、SQLite 文件锁和时钟粒度差异影响。权威 Linux CI 已通过三片离线测试与汇总 `backend-gate`，因此该本地平台差异不再阻断 G2。

### 2026-08-10 至 2026-08-11 生产切换与 canary 证据

- 初始四迁移、flag-off 切换及两阶段备份证据位于 `/opt/dsa-backups/personal-cutover-20260810T182351Z-05bdac1-retry2`；结果固定 10 条迁移、53 张表、Policy `off`，并保留迁移前后 SQLite/manifest 哈希。
- 新增 Skill Dataset lineage 迁移先在 `/opt/dsa-backups/personal-lineage-migration-rehearsal-20260811T032100Z-76d4df3b` 隔离演练，再由 `/opt/dsa-backups/personal-lineage-cutover-20260811T033527Z-76d4df3b` 完成生产迁移、幂等复核、迁移前后 SQLite/raw 配对归档与服务恢复。
- Outcome pending 行在父 Signal 过期后无法推进的 status-lineage 问题由 `f1aeb22b` 修复。迁移先在 `/opt/dsa-backups/personal-outcome-status-rehearsal-20260811T102300Z-f1aeb22b` 完成 11→12 marker、幂等、约束和六行 Outcome 不变演练；生产切换位于 `/opt/dsa-backups/personal-outcome-status-cutover-20260811T103422Z-f1aeb22b`，其 `SHA256SUMS` SHA-256 为 `ac47347a671731809f8d8c2092f62a9a949b78a8a2bccfc0f6307a6f63a44ebf`。首次因错误假设 raw 不存在而在迁移前 fail-closed 的 `/opt/dsa-backups/personal-outcome-status-cutover-20260811T102953Z-f1aeb22b` 原样保留，不作为成功证据。
- 当前代码身份由 `/opt/dsa-backups/personal-outcome-status-acceptance-20260811T104100Z-f1aeb22b` 固定；`acceptance.json` SHA-256 为 `21e1fd9ee72b97b07de645d0109c40abc3b8c6d4aa033407c7419114de740953`。server、analyzer、worker 均运行同一 `fa7e712b…16abe` 镜像，searxng 保持原容器，全部 healthy、restart 0。
- Thesis 正向验收为 `/opt/dsa-backups/personal-thesis-acceptance-20260811T053500Z-93c98dfb`；Policy Shadow 正向验收为 `/opt/dsa-backups/personal-shadow-canary-20260811T060622Z-93c98dfb`，对应 signal `122`、完整 Portfolio context、`would_block=false`。
- Outcome v2 正向提交与 pending 生命周期验收为 `/opt/dsa-backups/personal-outcome-shadow-promotion-20260811T062832Z-93c98dfb`：6 个 signal×horizon 候选均保持 `pending/entry_session_not_reached`，且 observation hash、lineage 与不可变状态完整。
- 修复后的生产 Outcome job `c85af905552348d5b224c72ebf354cc6` 首次成功处理 9/9 候选：原 6 行在父 Signal 过期后仍保持冻结 `signal_status=active` 并合法 pending，新建 3 行，未再发生 lineage conflict；活动任务与 Outbox 为 0，DB `quick_check=ok`、外键错误 0。
- Shadow Day 1 基线位于 `/opt/dsa-backups/personal-shadow-observation-acceptance-20260811T063353Z-93c98dfb`，固定 7 个 XSHG 交易日为 `2026-08-11/12/13/14/17/18/19`；自动门已通过，人工签字待完成。
- Shadow Day 1 收盘后只读观察位于 `/opt/dsa-backups/personal-shadow-observation-day1-20260811T072420Z-93c98dfb`，result SHA-256 为 `b1eaca0eef4ab26b2c30fdec390fdf687180d7f9867f12e71ab7aeb304680ec4`，SHA256SUMS SHA-256 为 `f98c6f0296473c6f3712449054ce10d53a25926032d1ee248240426854497918`；活动任务/Outbox 均为 0、Policy 无 `enforce`、6 个 Outcome 保持合法 pending，容器/API/DB 门全部通过。
- Day 2–7 自动采证已重新绑定最终 f1 身份：密封脚本位于 `/opt/dsa-development/pr6-shadow-observation-f1aeb22b/collect-policy-shadow-observation-f1aeb22b.sh`，SHA-256 为 `078d785a1323bd52edafce09b8c2333cb29830693047665b2bc4240bb0dd67b4`；六个持久化 `dsa-pr6-shadow-observation-dayN-f1aeb22b-persistent.timer` 分别固定到 `2026-08-12/13/14/17/18/19 15:20 CST`，主机重启后仍会恢复并补跑错过的触发点。脚本已通过 Bash/Python 语法、错误日期负例及线上只读预检，能校验当前 12 条迁移、9 个 Outcome、252 个 raw 引用与 170 个 raw 文件闭包；旧 93c 与 f1 transient timers 均已停止。持久化 unit 证据位于 `/opt/dsa-development/pr6-shadow-observation-f1aeb22b/systemd-units-20260811-f1aeb22b`，`installed-units.json` SHA-256 为 `7e5e4d94fdb7ab03ad7660ddbec6e4c72f8fb8735553ccfe7254aab4b48e1239`，`SHA256SUMS` SHA-256 为 `8f5a28faa4707b6c12ac1210393a783b1df41f2978916631866f2ef70e075de4`。
- Debate 日常开关仍保持关闭。任务 `a2716ca555ad4ed7be41f60f311cb963`（600519）和 `4003b08a028345b4981c963c2dde8d07`（601985）完成双 stance 与 Verifier 后被 Judge 以 `stance_confidence_below_minimum` 正确 fail-closed；随后 `f5b93315` 的 JSON-mode 修复以任务 `53d4a9873a544390bba427b41e237dec` 完成一次不放宽阈值的正向 Debate、Review、Thesis、Signal 与 Policy Shadow canary。独立证据位于 `/opt/dsa-backups/personal-debate-jsonmode-acceptance-20260811T094436Z-f5b93315`，`acceptance.json` SHA-256 为 `6499cf99c989e83c3757c8c590ab7d8ba3cff99159271f70fcdea18a6506396c`，因此 G8 已关闭。

## 门禁总表

| 门禁 | 验收范围 | 当前状态 | 必需证据 |
| --- | --- | --- | --- |
| G0 范围 | 原始 PR0–PR6 逐项映射，无编号漂移或隐藏排除项 | `PASS` | rollout 阶段表、CHANGELOG、配置依赖图 |
| G1 契约 | migrations、DB 约束、不可变 lineage、API/Pydantic、Web 类型、配置默认关闭 | `PASS` | 三路独立终审、静态 OpenAPI 与 runtime exact 对照、直接 SQL 反例 |
| G2 后端 | Python compile/lint、focused 回归、`ci_gate.sh`、非网络 pytest | `PASS` | 本地专项证据；GitHub CI run 31404437702 三片 backend-tests 与 backend-gate |
| G3 Web | `npm ci`、全量 ESLint、Vitest、TypeScript、生产构建、关键页面视觉证据 | `PASS` | 本地 1154 passed/2 skipped、ESLint/TypeScript/build；GitHub clean install/web-gate；PR 评论三图 |
| G4 迁移 | 旧库检查、全量 apply、幂等 apply、失败注入回滚、约束/trigger 直写反例 | `PASS` | 隔离演练与生产 migration evidence；12 markers、quick/FK 通过 |
| G5 备份恢复 | 迁移前旧工具备份、迁移后候选工具全表备份、raw 配对、异名隔离恢复 | `PASS` | 最新 f1 切换前后 SQLite/manifest 与 170 个 raw artifact 配对证据及异名恢复 |
| G6 镜像与编排 | Docker build/import smoke、normal/durable profiles、Worker heartbeat、唯一 Scheduler owner | `PASS` | 固定 f1 image/OCI revision/Compose；server 抑制调度，analyzer 为唯一 owner，worker healthy |
| G7 flag-off 部署 | 新 schema/image 上线但新增研究 flags 全关，旧 API/任务/通知无回归 | `PASS` | flag-off 首次上线、readiness/API/DB/容器身份及逐阶段可逆切换证据 |
| G8 分阶段 canary | Durable → Tushare → Factors → Evidence → Skill → conditional Debate/Thesis → Outcome v2 | `PASS` | Debate 安全负例与一次不放宽阈值的正向 Review/Thesis 均通过；Outcome f1 job 9/9 成功 |
| G9 Policy Shadow | 7 个真实交易日 shadow；差异、误阻断、缺失估值、账户动作和回滚均复核 | `IN_PROGRESS` | Day 1 基线与收盘后观察均已封存；Day 2–7 的 f1 定时采证已武装，2026-08-12 至 19 仍需逐日证据与人工签字 |

## PR0–PR6 功能签收

| 阶段 | 必须证明的业务结果 | 离线 | 生产 |
| --- | --- | --- | --- |
| PR0 | 显式迁移、readiness、配置依赖、默认关闭、可恢复基线 | `PASS` | `PASS` |
| PR1 | Durable Worker、lease/heartbeat、JobEvent、Outbox、唯一 Scheduler owner | `PASS` | `PASS` |
| PR2 | Worker-only Tushare、冻结 Dataset/Factor、限流、raw 可恢复 | `PASS` | `PASS` |
| PR3 | watchlist/legacy/holding union、Opening/Reconciliation、预算、Policy shadow/enforce | `PASS` | `IN_PROGRESS`（Shadow Day 1/7；Enforce 禁止） |
| PR4 | 五 Skill、模式/预算、条件 Debate、Verifier/Judge、immutable Thesis、durable resume | `PASS` | `PASS`（安全负例与正向 Debate/Review/Thesis canary 均通过） |
| PR5 | T+1、5/10/20d、复权、MFE/MAE、CSI300/SW1 点时基准、四维校准 | `PASS` | `IN_PROGRESS`（9 个 Outcome 已正确 pending；成熟窗口与 n≥30 尚未到达） |
| PR6 | Web、OpenAPI、调度、通知、备份、部署与运维闭环 | `PASS` | `IN_PROGRESS`（仅 G9 七交易日 Shadow 尚未关闭） |

## Canary 与 Shadow 记录模板

每一行必须对应真实交易日和固定生产版本。任务未运行、Provider 权限不足、没有成熟 20d 样本或没有足够校准样本时，按真实原因记录，不得填零或跳过。

| 交易日 | 版本/镜像 | 开启阶段 | canary task/job | Provider 与 lineage | Policy shadow allow/would-block | Outcome mature/pending/unable | 异常与处置 | 复核人 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-08-11（Day 1） | `93c98dfb` / `0474a075…aec22` | Evidence、Skills、Thesis、Outcome v2、Policy Shadow；Debate off | Shadow `1761a658…ac1fc`；Outcome `fdb0dcef…5506e` | Dataset/Factor/Evidence/Skill/Thesis lineage 通过；Outcome historical replay 无 Provider 漂移 | `no_action` / `would_block=false`，Portfolio context 完整 | 6 pending，均为 `entry_session_not_reached`，没有伪造 0 | 两次 Debate Judge 低置信度 fail-closed；生产自动恢复且健康 | 基线+收盘后自动门 PASS；人工待签 |
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

截至 2026-08-11，固定生产版本 `f1aeb22b` 已完成 G4–G8 并运行于服务器；迁移、SQLite/raw 配对恢复、镜像编排、flag-off 回归以及 Evidence、Skills、条件 Debate/Review、Thesis、Outcome v2 与 Policy Shadow 的生产 canary 均已通过。G9 仍处于 Day 1/7，因此当前准确结论是：**系统已正常上线且 canary 验收完成，但七交易日 Shadow 尚未完成，不可启用 Policy Enforce。** 后续每关闭一个门禁，都必须继续回填固定版本、哈希和生产证据；仅更新叙述而不附可复核证据不构成验收。
