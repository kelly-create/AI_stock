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
