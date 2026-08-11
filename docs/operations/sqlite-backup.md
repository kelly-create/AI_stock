# SQLite 在线备份、校验与恢复

`scripts/sqlite_backup.py` 用于生产切换前后的 SQLite 一致性备份。它通过
`sqlite3.Connection.backup()` 读取活动数据库，因此能够在 WAL writer 仍在提交时取得一致快照，
不会直接复制可能与 `-wal` 文件不一致的主数据库文件。

工具默认使用 PR0 的核心表清单，并复用生产基线的检查口径。发布的 manifest 不包含绝对路径或业务行，
只包含：备份文件名、SHA-256、字节数、UTC 时间、迁移版本、Schema/Index canonical hash、核心表行数、
`quick_check` 和 `foreign_key_check` 结果。缺少核心表或任一完整性检查失败时，不会发布备份。
PR1 起默认核心表还强制包含 `analysis_jobs`、`job_events`、`notification_outbox` 和
`provider_health`，避免只备份业务结果却遗漏正在执行的任务、通知或组件健康状态。
PR2 起还强制包含 `research_dataset_snapshots`、`research_factor_snapshots` 和
`research_snapshots`，使迁移、恢复和生产切换能够逐表核对研究数据、确定性因子与冻结快照行数。
个人投研 PR3–PR5 起还强制包含关注池、Reconciliation 主表与调整明细、研究预算、Portfolio Policy
Evaluation、Skill Contract/Execution、Debate Review、Thesis 和 Decision Outcome v2。备份与恢复验收会逐表
核对这些表的存在性和行数；只验证旧业务报告表或只验证数据库文件哈希，均不能作为本阶段的生产恢复证据。
manifest 另含一个排除自身字段后计算的 canonical SHA-256，用于发现文件内容的意外改写；它不是签名，不能替代
对 manifest 文件本身的只读保管或外部校验和记录。

## 创建备份

跨迁移发布必须保留两份不同阶段的证据：迁移前旧生产库使用**当前已部署版本**的密封备份工具及其旧 Schema
核心表清单创建 rollback 备份；迁移完成后，再使用**候选版本**的默认清单创建 post-migration 备份，并确认上述
PR3–PR5 新表全部存在且行数已记录。候选版本的新默认清单会在尚未迁移的旧库上因缺表而正确失败，不得通过
临时缩减候选清单来伪装成迁移前备份。两份 manifest、工具版本/源码哈希和外部校验和都必须进入验收记录。

输出目录必须已经存在，备份和 manifest 必须位于同一目录，两个目标都必须不存在：

```bash
python scripts/sqlite_backup.py backup \
  --database /srv/dsa/data/stock_analysis.db \
  --output /srv/dsa/backups/stock-analysis-20260808.sqlite
```

成功后同时得到：

- `stock-analysis-20260808.sqlite`
- `stock-analysis-20260808.sqlite.manifest.json`

备份与 manifest 均先写入同目录临时文件，执行文件 `fsync` 后再通过同文件系统原子硬链接发布；若目标在预检后
被另一个进程创建，发布会失败且不会覆盖它。只有 manifest 成功发布后，
这一组文件才视为可恢复备份；发布 manifest 失败时工具会撤销本次刚发布的备份文件。进程被强制终止后若
留下无 manifest 的备份或隐藏 `.tmp` 文件，校验和恢复均会失败关闭，不得手工补写 manifest。

online backup 的输出会统一切换为 `journal_mode=DELETE`，使发布物成为不依赖 sidecar 的单文件快照。输出或
manifest 不得使用源库的 `-wal`、`-shm`、`-journal` 名称，二者也不得互相使用这些 sidecar 名称。

仅在隔离测试库使用非生产 Schema 时，可以重复传入 `--core-table` 替换默认核心表集合。生产备份不得缩减
默认集合来绕过缺表检查。

## 严格校验

```bash
python scripts/sqlite_backup.py verify \
  --backup /srv/dsa/backups/stock-analysis-20260808.sqlite
```

校验会重新计算文件大小和 SHA-256，并重新打开备份核对迁移版本、Schema、索引、逐个核心表行数和两项
完整性检查。备份文件被重命名、截断，manifest canonical 内容被意外修改，或者数据库内容与 manifest 不一致时
都会失败。校验与恢复也会拒绝备份旁的任意 `-wal`、`-shm`、`-journal`，防止校验看到 WAL 叠加视图、恢复却
只复制主文件。若有意重新计算 manifest 自身 hash，则该字段不能提供真实性保证；真实性必须依赖外部校验和或签名。

## 隔离恢复演练

默认恢复只允许写入一个不存在的隔离目标；父目录必须预先创建，工具不会创建或清理宽泛目录：

```bash
mkdir -p /srv/dsa/restore-drill
python scripts/sqlite_backup.py restore \
  --backup /srv/dsa/backups/stock-analysis-20260808.sqlite \
  --target /srv/dsa/restore-drill/stock_analysis.db
```

恢复前会完整验证源备份，写入同目录临时文件并再次验证，最后才以不覆盖方式原子发布目标。已有目标文件以及同名
`-wal`、`-shm`、`-journal` sidecar 会被拒绝，工具不会覆盖或删除它们。恢复后应使用应用候选镜像只读检查
核心 API、历史报告、Portfolio、Decision Signal、Outcome v1，以及个人投研关注池/Reconciliation、
Policy/Skill/Thesis 和 Outcome v2 的只读查询与 lineage 闭包。

## 显式替换生产数据库

生产替换是独立的危险模式，必须同时满足：

1. API 已停止接收任务，Worker 和 Scheduler 已停止，所有数据库使用者均已退出。
2. 已完成最终 `PRAGMA wal_checkpoint(TRUNCATE)`，目标旁不存在 `-wal`、`-shm` 或 `-journal`。
3. `--target` 是绝对路径、当前生产数据库是普通文件，且备份与目标位于同一文件系统。
4. 操作者显式提供 `--replace-production --services-stopped`；后一个参数是停机声明，不是自动探测。

```bash
python scripts/sqlite_backup.py restore \
  --backup /srv/dsa/backups/stock-analysis-20260808.sqlite \
  --target /srv/dsa/data/stock_analysis.db \
  --replace-production \
  --services-stopped
```

替换前，工具会使用 SQLite online backup 在目标同目录保存旧库：
`stock_analysis.db.pre-restore-<UTC>.sqlite`，并为它生成配套 `.manifest.json`。旧库备份对通过 hash、Schema、
逐表计数和完整性检查并原子发布后，候选库才会替换生产目标；旧库备份对不会自动删除。替换后按迁移计划
依次启动 API readiness、Worker heartbeat 和 Scheduler，通知保持关闭直到 canary 通过。

需要完整回滚时，再次停机并确认 sidecar 已清除，可以把该 `pre-restore` 备份及其自动生成的 manifest 作为
恢复输入。日常操作仍应优先使用切换前按发布流程保存的标准备份对；不要让旧镜像直接运行已迁移数据库。

## 退出码与故障处理

- `0`：创建、校验或恢复成功。
- `1`：输入契约、hash、Schema、计数或完整性检查失败。
- `2`：未分类的操作系统错误；为避免泄露宿主路径，CLI 不打印原始绝对路径。

正常的预检、复制或发布失败会保留现有生产数据库不动；若明确出现
`restore failed and automatic production rollback failed`，不得假定目标状态，应保持服务停止，并使用已保留的
`pre-restore` 备份对进行人工恢复。不得通过删除 manifest 字段、缩减生产核心表集合、手工改 hash，或强行删除
活动 sidecar 来绕过检查。

文件内容在 Windows 和 POSIX 都会显式 `fsync`。POSIX 还会在发布和自动回滚后 `fsync` 父目录；Python 标准库
在 Windows 上没有等价的目录 `fsync`，因此要求断电级目录元数据持久性的正式切换应在 Linux 生产环境执行，
Windows 本地执行只能作为功能演练，不能替代 Linux 恢复验证。
