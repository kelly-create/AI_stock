# 研究原始数据保留与清理 / Research raw retention

`research_dataset_snapshots.normalized_json` 和不可变 Research Snapshot 自含研究实际消费的标准化值，不依赖原始文件继续存在。原始供应商响应是独立的内容寻址归档，默认位于数据库同级的 `research/raw/`；SQLite 在线备份不包含该目录，需要完整取证恢复时必须单独备份。

`research_dataset_snapshots.normalized_json` and immutable Research Snapshots contain the normalized values actually consumed by research. They remain readable after raw provider artifacts are removed. Raw responses are a separate content-addressed archive under `research/raw/` beside the database by default and must be backed up separately from SQLite when full forensic recovery is required.

## 保留策略 / Retention policy

| 类型 / Class | 数据集 / Datasets | 保留 / Retention |
| --- | --- | --- |
| 行情 / Market | `daily`, `adj_factor`, `daily_basic`, `cyq_perf`, `cyq_chips`, `stk_limit`, `suspend_d` | 30 天 / days |
| 搜索与新闻 / Search and news | `search`, `search_results`, `news`, `news_search`, `articles`, `intelligence_items` | 90 天 / days |
| 财务、事件及主数据 / Financial, event and reference | `stock_basic`, `fina_indicator`, `income`, `balancesheet`, `cashflow`, `dividend`, `stk_holdernumber`, `forecast`, `event(s)`, `announcements`, `disclosures` | 长期 / permanent |
| 未知数据集 / Unknown dataset | 任何未显式登记的名称 / any unregistered name | 保守长期 / conservatively permanent |

有限保留期使用 `observed_at`、`created_at` 与最近一次 `research_dataset_snapshot` Job binding 时间中的最晚值。这样不可变数据集按内容去重后，即使同一供应商响应被重新抓取，也会从最近一次任务引用起重新计算 30 天行情保留期。恰好位于 30/90 天截止点的引用仍保留，只有严格早于截止点才过期。同一 `content_sha256` 被多条记录引用时，只要任一引用未过期或属于长期类型，该哈希对应的全部候选路径都保留。

Finite retention uses the latest of `observed_at`, `created_at`, and the newest `research_dataset_snapshot` job binding. Therefore, immutable content deduplication does not make a freshly reused provider response immediately old for the current 30-day market policy. A reference exactly on the 30/90-day cutoff is retained; only strictly older references expire. When several rows reference one `content_sha256`, any unexpired or permanent row protects every candidate path for that hash.

A crash or stale lease can leave a content-addressed raw file after publication but before its database binding commits. The maintenance plan therefore enumerates the canonical raw root while holding the same Worker-owner and database locks used for apply. A valid gzip file with no snapshot reference becomes an orphan candidate only when its filesystem modification time is strictly more than 24 hours old. Newer files remain protected; malformed gzip or a filename/content hash mismatch blocks the entire apply before any move.

Job Event 按 30 天保留，因此该延展只覆盖当前 Tushare 行情数据的 30 天策略。PR3 Evidence 仅将 bounded normalized 搜索 snippet 写入 SQLite Dataset，`raw_ref=None`，不创建 result-page/raw sidecar。未来若引入 90 天搜索/新闻 raw sidecar，必须增加独立的 raw reference occurrence 或 `last_observed` 元数据，不能用已经清理的 30 天事件推导 90 天边界。

Job Events are retained for 30 days, so this extension is deliberately limited to the current 30-day Tushare market policy. PR3 Evidence stores only bounded normalized search snippets in a SQLite Dataset with `raw_ref=None` and creates no result-page/raw sidecar. If a future release adds a 90-day search/news raw sidecar, it must add a dedicated raw-reference occurrence or `last_observed` record rather than infer a 90-day boundary from 30-day events.

## 执行 / Operation

默认命令仅生成 JSON 计划，不删除文件：

The default command only emits a JSON plan and never deletes files:

```bash
python scripts/prune_research_raw.py
```

生产执行前建议固定检查时间并保存 dry-run 输出，复核后显式应用：

For production, pin the planning time, retain the dry-run output, review it, and then apply explicitly:

```bash
python scripts/prune_research_raw.py --as-of 2026-08-08T00:00:00Z
python scripts/prune_research_raw.py --as-of 2026-08-08T00:00:00Z --apply
```

dry-run 可使用 `--database-path` 和 `--raw-root` 检查隔离副本。`--apply` 只接受 `.env`/`ENV_FILE` 解析出的 active `DATABASE_PATH` 精确指向的文件，以及该数据库同级的规范 `research/raw/`；即使备份库与生产库位于同一目录、共享相同 raw root，也不得对备份库执行 apply。其他数据库和外部 raw root 只允许 dry-run。对隔离副本执行 apply 时，必须同时提供隔离的 `ENV_FILE`，其中 `DATABASE_PATH` 指向该副本，且副本使用自己相邻的 raw 目录；不得沿用生产环境文件或共享生产 raw 目录。未指定数据库时会先加载仓库 `.env`/`ENV_FILE`，再读取 `DATABASE_PATH`（默认 `./data/stock_analysis.db`）。active 数据库不存在或不是普通文件时 fail-closed 返回 `1`。

Dry runs may use `--database-path` and `--raw-root` to inspect an isolated copy. `--apply` requires the selected database to be the exact file resolved as the active `DATABASE_PATH` from `.env`/`ENV_FILE`, and only accepts its adjacent canonical `research/raw/`. A backup remains dry-run-only even when it sits beside production and therefore shares the same raw root. To apply against an isolated copy, supply an isolated `ENV_FILE` whose `DATABASE_PATH` points to that copy and give it its own adjacent raw directory; never reuse the production environment file or production raw root. Without a database override, the command loads `.env`/`ENV_FILE` before reading `DATABASE_PATH` (default `./data/stock_analysis.db`). A missing or non-file active database fails closed with exit code `1`.

`--apply` 先非阻塞获取 Durable Worker 使用的同一 `数据库文件名.tushare-owner.lock`，因此 Worker 在线或正在写 RawArtifactStore 时会返回 `1`，不会等待或修改文件。取得 owner 后才执行 SQLite `BEGIN IMMEDIATE`，并在两把锁内重新读取全部引用、生成计划和提交文件移动。命令只接受 `<hash-prefix>/<sha256>.<extension>.gz` 形式、位于指定 raw root 内且不经过符号链接的安全相对路径；所有现存引用必须是普通 gzip 文件且解压内容 SHA-256 匹配，受保护/长期引用还必须存在。损坏 JSON、哈希/路径不匹配、路径穿越、缺失的受保护文件、损坏 gzip、非普通文件或时间字段损坏会在移动前阻断整次 apply，返回 `2` 且不删除任何候选。已缺失的过期文件只报告在 `missing_paths`，不会阻断。

`--apply` first acquires the same non-blocking `database-name.tushare-owner.lock` as the Durable Worker. If a Worker is online or may be writing RawArtifactStore, the command exits `1` without waiting or modifying files. Only then does it start SQLite `BEGIN IMMEDIATE`; the complete reference reload, plan, and filesystem staging commit run inside both locks. Every existing reference must be a regular gzip file whose decompressed SHA-256 matches its reference, and protected/permanent artifacts must also exist. Malformed JSON, hash/path mismatch, traversal, missing protected data, corrupt gzip, non-regular targets, or invalid timestamps block before any move with exit code `2`. Already-missing expired artifacts remain non-blocking `missing_paths` entries.

这是停写维护命令，不是在线垃圾回收器。dry-run 可以在线执行；正式 `--apply` 前必须先暂停 Durable Worker，并停止 API/Scheduler/Bot 产生数据库写流量。脚本会用 owner 锁验证 Worker 已退出，并在完整引用校验与 staging 期间持有 `BEGIN IMMEDIATE`；若 API 写流量仍在，可能触发 SQLite busy timeout。任何锁获取/超时错误都返回 `1`，且不会移动 raw 文件。

This is a write-quiesced maintenance command, not an online garbage collector. Dry-run is safe online. Before `--apply`, stop the Durable Worker and quiesce API/Scheduler/Bot database writes. The owner lock verifies that the Worker has exited, and `BEGIN IMMEDIATE` remains held throughout reference verification and staging. Leaving API writes active can exhaust their SQLite busy timeout. Any owner/database lock acquisition or timeout error returns `1` without moving raw files.

候选文件先通过同文件系统原子 rename 移入规范 raw root 内的 `.retention-staging-<随机值>/`；该目录名不可能匹配内容路径。全部移动完成前若失败，本次已移动文件会反向恢复。全部移动成功后规范路径即视为已提交删除，再清理 staging。若 purge 失败，输出模式为 `committed_with_cleanup_error`、返回 `1`，`deleted_paths` 表示已经从规范位置移除的路径，`cleanup_pending_paths` 和 `staging_directory` 指出仍可恢复的残留；不得把该状态理解为回滚成功。规范路径提交后若 SQLite 事务释放失败，也保留同一 committed 状态与 `deleted_paths` 并返回 `1`，不会退化为声称未修改文件的通用错误。发现上次运行遗留的 staging 时，新 apply 会返回 `1` 并要求先恢复或清理。正常 dry-run/apply 返回 `0`，预检阻断返回 `2`，其他数据库/文件系统/锁错误返回 `1`。

Candidates are atomically renamed into `.retention-staging-<random>/` inside the canonical raw root before purge; that directory name cannot match a content path. A failure before all moves finish rolls this run's moves back. Once every move succeeds, canonical deletion is committed and staging is purged. A purge failure returns `1` with mode `committed_with_cleanup_error`: `deleted_paths` are already absent from canonical locations, while `cleanup_pending_paths` and `staging_directory` identify recoverable remnants. It must not be treated as a successful rollback. If SQLite transaction release fails after canonical paths are committed, the command preserves the same committed status and `deleted_paths` and returns `1` instead of degrading to a generic no-change error. A later apply refuses to proceed while an earlier staging directory exists. Clean dry-run/apply returns `0`, preflight blocking returns `2`, and other database/filesystem/lock errors return `1`.

`--as-of` 必须带 `Z` 或显式 UTC offset；无时区时间会返回 `1`，避免本地墙上时间被静默解释成 UTC 而提前清理。数据库中的 SQLite naive timestamp 仍按其既有 UTC 契约解释。

`--as-of` must include `Z` or an explicit UTC offset. A naive wall-clock value exits `1` rather than being silently treated as UTC and moving the cutoff. Existing naive SQLite timestamps retain their established UTC interpretation.

清理不会删除 `research_dataset_snapshots` 行、`normalized_json`、Factor Snapshot 或 Research Snapshot。若要回滚原始文件删除，只能从单独的 raw 归档恢复；因此正式 apply 前必须先完成并校验该目录备份。

Retention never deletes `research_dataset_snapshots` rows, `normalized_json`, Factor Snapshots, or Research Snapshots. Raw deletion can only be reversed from the separately archived raw directory, so validate that backup before production apply.
