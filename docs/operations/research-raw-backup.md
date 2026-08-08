# Research 原始取证归档、校验与隔离恢复

`scripts/research_raw_archive.py` 将一份已经完成的 SQLite 在线备份与完整的
`research/raw/` 内容寻址目录绑定成可校验的取证备份组。这个工具用于需要保留供应商原始响应的完整取证恢复；
普通业务恢复仍可只依赖 SQLite 中的 `normalized_json`、Factor Snapshot 和 Research Snapshot。

取证备份组必须同时保存三项：

- `scripts/sqlite_backup.py` 生成并已校验的 SQLite 单文件备份；
- 未压缩 tar 归档，其中每个成员仍是 RawArtifactStore 写出的 gzip 文件；
- raw manifest，包含 SQLite 备份 SHA-256、archive SHA-256、全部文件路径、压缩内容 SHA-256、
  解压内容 SHA-256、压缩/解压字节数和数据库引用集合。

manifest 的 canonical SHA-256 用于发现意外改写，不是数字签名。SQLite 备份、raw archive 和 manifest
应放入只读/不可变备份存储，并在外部记录或签名它们的摘要；不能让攻击者同时改写三者及校验记录。

## 创建完整取证备份组

创建前暂停 Durable Worker，并停止 API、Scheduler、Bot 和其他可能写 SQLite 或 `research/raw/` 的进程。
先创建 SQLite 在线备份，再在仍保持停写的窗口内创建 raw archive：

```bash
mkdir -p /srv/dsa/backups

python scripts/sqlite_backup.py backup \
  --database /srv/dsa/data/stock_analysis.db \
  --output /srv/dsa/backups/stock-analysis-20260808.sqlite

python scripts/research_raw_archive.py create \
  --raw-root /srv/dsa/data/research/raw \
  --database-backup /srv/dsa/backups/stock-analysis-20260808.sqlite \
  --archive /srv/dsa/backups/research-raw-20260808.tar
```

成功后会发布：

- `research-raw-20260808.tar`
- `research-raw-20260808.tar.manifest.json`

archive 与 manifest 必须位于同一个已存在目录，且两个目标都必须不存在。工具先写入并 `fsync` 临时文件，
再以不覆盖方式发布；第二项发布失败时会撤销本次刚发布的第一项并清理临时文件。输出路径、SQLite 备份
不得位于 raw root 内。

`create` 会全量枚举 raw root，而不是只归档数据库当前引用的文件。它只接受
`<sha256前两位>/<sha256>.<小写扩展名>.gz`，并拒绝符号链接、junction、嵌套目录、retention staging、
`.tmp` 和任何其他非规范入口。每个 gzip 都会实际解压，解压内容 SHA-256 必须与目录及文件名一致；
SQLite 备份中的每条非空 `research_dataset_snapshots.raw_ref_json` 还必须指向 archive 中的准确路径、hash
和字节数。未被数据库引用但仍位于规范 raw root 的文件也会被归档。

这是停写备份命令，不是在线快照器。工具会检测枚举期间的常规新增、删除或改写，但不能替代停止写入；
如果无法证明 Worker 和所有写入方已经退出，不得把产物作为完整取证备份签收。

## 严格校验

每次复制、上传、下载或恢复前后都同时提供原 SQLite 备份：

```bash
python scripts/sqlite_backup.py verify \
  --backup /srv/dsa/backups/stock-analysis-20260808.sqlite

python scripts/research_raw_archive.py verify \
  --archive /srv/dsa/backups/research-raw-20260808.tar \
  --database-backup /srv/dsa/backups/stock-analysis-20260808.sqlite
```

raw 校验会重新计算 archive/SQLite/manifest 身份，执行 SQLite `quick_check`，重新读取数据库 raw 引用，
逐个检查 tar 成员路径、类型、元数据、压缩 hash、解压 hash 和字节数。任一文件被截断或改写、路径穿越、
重复/额外成员、非普通文件、损坏 gzip、SQLite 引用缺失、SQLite sidecar，或 manifest 与数据库引用集合不一致，
都会 fail closed。

若 manifest 使用非默认位置，三个命令都可以显式传入 `--manifest /path/to/raw.manifest.json`。
archive 改名后会因文件名身份不一致而失败。SQLite 恢复副本可以使用不同文件名；它仍必须与 manifest 中的
SHA-256、字节数和 raw 引用集合完全一致。不要手工修改 manifest 来绕过检查。

## SQLite + raw 完整隔离恢复顺序

恢复始终先落到全新的隔离路径。下面两个父目录必须预先存在，目标数据库文件和 raw 目标目录必须不存在：

```bash
mkdir -p /srv/dsa/restore-drill/data/research

# 1. 先严格校验备份组
python scripts/sqlite_backup.py verify \
  --backup /srv/dsa/backups/stock-analysis-20260808.sqlite
python scripts/research_raw_archive.py verify \
  --archive /srv/dsa/backups/research-raw-20260808.tar \
  --database-backup /srv/dsa/backups/stock-analysis-20260808.sqlite

# 2. 恢复绑定的 SQLite
python scripts/sqlite_backup.py restore \
  --backup /srv/dsa/backups/stock-analysis-20260808.sqlite \
  --target /srv/dsa/restore-drill/data/stock_analysis.db

# 3. 恢复绑定的完整 raw root
python scripts/research_raw_archive.py restore \
  --archive /srv/dsa/backups/research-raw-20260808.tar \
  --database-backup /srv/dsa/backups/stock-analysis-20260808.sqlite \
  --target-root /srv/dsa/restore-drill/data/research/raw

# 4. 对恢复后的文件再次执行校验，并运行应用候选镜像的只读 smoke
python scripts/research_raw_archive.py verify \
  --archive /srv/dsa/backups/research-raw-20260808.tar \
  --database-backup /srv/dsa/restore-drill/data/stock_analysis.db
```

第 4 步只有在隔离 SQLite 与原备份字节完全相同时才会通过，这正是
`scripts/sqlite_backup.py restore` 的契约。随后应使用隔离配置确认 Research Dataset/Snapshot 查询、历史报告、
Portfolio、Decision Signal 和 Outcome 等只读路径，并抽样通过 `RawArtifactStore.read` 读取数据库引用。

raw `restore` 永远不会替换已有目录，也没有生产覆盖开关。它先在目标父目录构造并校验 staging，随后以
`mkdir(exist_ok=False)` 原子认领不存在的目标，再发布 shard；任一步失败会清理本次 staging 和已认领的部分目标，
而并发创建的目标不会被覆盖或删除。

全新灾备主机可将不存在的规范 raw 路径直接作为 `--target-root`。已有生产主机必须先完成上述隔离恢复和
只读验收，再按 [SQLite 在线备份、校验与恢复](sqlite-backup.md) 的停机规则处理数据库。raw 切换时保持所有服务
停止，在同一文件系统内保留旧 raw 目录为带时间戳的回滚目录，并由运维人员将已验收的隔离目录原子改名到规范
`research/raw/`；本工具不会代替该显式生产切换，也不会删除旧目录。SQLite 与 raw 必须来自同一 manifest 绑定组，
不得混用不同时间点的产物。

## 15 分钟 RTO 目标与演练

目标是 **RTO 不超过 900 秒**。该目标的计时范围为：备份文件已经在恢复主机本地可用后，从开始严格校验，
经过 SQLite 隔离恢复、raw 隔离恢复和规定的只读 smoke，到候选数据组具备切换条件为止。事故发现、审批、服务停写、
异地介质下载、网络传输、新主机供给、DNS/流量切换不在这 900 秒内，应分别度量并纳入更上层灾备 SLO。

`create`、`verify`、`restore` 每次成功都会输出 JSON `elapsed_seconds`。仓库测试使用小型真实 SQLite + gzip/tar
备份组执行完整演练并断言总耗时小于 900 秒；它只证明功能和计时契约，不代表生产容量达标。生产至少每月使用
代表性数据量在 Linux、同类磁盘和候选镜像上演练，记录：

1. 两个 verify、SQLite restore、raw restore 和只读 smoke 的开始/结束 UTC 时间；
2. 每个 CLI 的 `elapsed_seconds`、SQLite + raw 完整恢复总 wall-clock、数据库/archive 字节数和 artifact 文件数；
3. 主机、文件系统、磁盘类型、候选版本和校验摘要；
4. 是否在 900 秒内完成，以及超时后的容量、存储或流程整改负责人。

任何代表性演练超过 900 秒、未覆盖同一绑定组、缺少只读 smoke，或只在 Windows 上通过，都不能签收生产 RTO。
正式断电级持久性和原子目录切换演练应在 Linux 生产等价环境完成。

## 退出码与故障处理

- `0`：创建、校验或隔离恢复成功；标准输出是包含 `elapsed_seconds` 的 JSON。
- `1`：输入、hash、gzip、tar、SQLite 引用、路径或安全契约失败。
- `2`：未分类操作系统错误；为避免泄露主机路径，CLI 不打印原始绝对路径异常。

失败后不要删除字段、重算 manifest 或使用解压工具手工忽略危险成员。保留原备份组和 stderr，修复来源或重新创建
完整绑定组。恢复失败时若报告 cleanup incomplete，保持服务停止并人工检查目标父目录中的
`.目标名.restore-*` 与部分目标；在状态查明前不得将其当作可用恢复结果。
