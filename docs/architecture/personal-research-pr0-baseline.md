# 个人投研 PR0：生产基线与 Golden 契约

状态：Accepted
适用阶段：个人 A 股投研二开 PR0 及后续生产候选验证

## 决策

二开开始前必须先形成一份可复核、不可包含凭据的生产基线，并把现网 `/app` 源码与目标 Git commit 对账。基线是发布证据，不是运行配置，也不得提交包含现网信息的一次性采集结果。

本节描述的基线采集与对账工具本身只读，不修改数据库、不执行迁移、不改变服务启动方式。完整 PR0 同时包含版本化 migrator、readiness 与 Compose 启动收口，见[部署与迁移说明](../DEPLOY.md)和[个人研究发布说明](../personal-research-rollout.md)：

- Git 记录 commit、branch 和 dirty 状态，不记录 remote URL。
- 镜像只读取 reference、image ID、RepoDigest 和 OCI revision label；禁止读取完整 `docker inspect`，避免把容器环境变量带入产物。
- Compose 和 runtime 配置只做原始字节 SHA-256。工具不解析、不保存、不打印配置键值。
- SQLite 通过 `mode=ro` 和 `PRAGMA query_only=ON` 读取；记录 Schema/Index 哈希、核心表行数、`quick_check` 和 `foreign_key_check`，不导出业务行内容。默认清单（包含 `portfolio_daily_snapshots`）是生产验收的 required 集合，任一缺表都会生成失败报告并返回非零退出码。
- 源码清单只覆盖 Dockerfile 复制的后端源码边界：根目录 Python 文件、`requirements.txt`、`api/`、`bot/`、`data_provider/`、`src/`、`strategies/`。运行数据和缓存不进入清单；如果该 COPY 范围内出现 `secrets.*`、credentials、runtime env 或密钥/证书后缀，工具会在读取文件内容前安全失败，而不是静默跳过。
- 构建后的 `static/` 使用独立 `docker-static-assets-v1` profile。源码一致不代表前端构建产物一致，两条证据必须分开生成。
- 为避免 Windows `core.autocrlf` 与 Linux 镜像造成整树误报，已知文本文件只规范化 CRLF/LF 后计算哈希；二进制文件仍按原始字节比较，其它空白差异不会被忽略。

字段、策略、Prompt 与研究快照的首版标识固定为：

| 契约 | 版本 |
| --- | --- |
| 字段字典 | `research-fields-v1` |
| Policy | `personal-policy-v1` |
| Prompt | `personal-research-v1` |
| Research Snapshot | `research-snapshot-v1` |

后续任何影响研究语义的变更必须提升对应版本；相同输入和全部版本必须产生相同 Snapshot Hash。

## 基线采集

先在生产数据库的隔离副本上运行，不直接操作现网文件。一次性 JSON 应保存到受控的发布证据目录或 CI artifact，不应加入仓库：

```bash
python scripts/capture_production_baseline.py \
  --database /secure-copy/stock_analysis.db \
  --compose docker/docker-compose.yml \
  --config-file /secure-copy/runtime.env \
  --container daily-stock-server \
  --output /release-evidence/pr0-baseline.json
```

需要自定义 required 表时可重复使用 `--core-table`。一旦显式提供该参数，它会替代默认表集合，并继续采用严格验收：任何指定表缺失都会先写出带 `missing_required_core_tables` 的 JSON，再返回退出码 `1`。仅做旧库盘点时必须显式追加 `--allow-missing-core-tables`；它只把缺表降为 `mode=diagnostic/status=warning`。`quick_check` 非 `ok`、issue count 非零、`foreign_key_check` 非 `ok` 或存在任一违例时，始终写出 `status=failed` 与 `failed_integrity_checks` 并返回 `1`，该开关不能放宽。输入或采集错误返回 `2`。

正式模式必须同时提供 `--container`/`--image-ref`、至少一个 `--compose` 和至少一个 `--config-file`；任一身份项缺失都会写入 `missing_identity_artifacts` 并返回 `1`。没有 Docker daemon 的离线盘点只有显式追加 `--diagnostic` 才可省略这些身份项，此时输出只能作为诊断材料，不能作为生产通过证据。该开关同样不会放宽 SQLite 完整性检查。

配置指纹会读取文件字节并只输出摘要。采集日志和最终 JSON 都不会包含配置内容或文件所在的宿主目录。

## 现网源码对账

把容器 `/app` 复制到隔离临时目录后，在仓库侧执行：

```bash
python scripts/source_manifest.py compare \
  --repo-root . \
  --deployed-root /isolated/exported-app \
  --output /release-evidence/source-comparison.json
```

退出码语义：

- `0`：源码 profile 完全一致。
- `1`：存在缺失、额外或内容哈希不一致的文件；JSON 会列出相对路径与哈希，但不包含文件正文。
- `2`：输入或采集本身失败。

发现漂移时必须先逐项判断是现网补丁、构建差异还是无效运行产物。只有实际应用源码补丁需要带回仓库并补回归测试；不得把整个 `/app` 反向覆盖仓库。

源码 profile 有意不包含构建后的 Web 文件。应从同一个生产候选镜像或其受控构建 artifact 取得 expected `static/`，再与部署容器导出的 `/app/static` 比较：

```bash
python scripts/source_manifest.py compare-assets \
  --expected-static-root /release-candidate/static \
  --deployed-static-root /isolated/exported-app/static \
  --output /release-evidence/static-comparison.json
```

`compare-assets` 的退出码同样为一致 `0`、漂移 `1`、采集失败 `2`。不要重新执行一次 Web build 后直接与旧镜像比较，因为 `build-info.json` 含本次构建时间；expected 与 deployed 必须来自同一个不可变构建产物链。

## Golden 契约目录

`tests/fixtures/research_golden/` 是完全固定、机器可读且不依赖实时市场的 PR0 契约目录；它锁定字段、状态、时间边界与执行语义，但还不是可直接驱动完整研究 API/报告的数值型 canary：

- `field_dictionary.json` 固定字段、状态枚举与必填字段。
- `cases.json` 固定 10 个 contract case 的数据集状态、时间与执行语义：`600519`、`601398`、`300750`、保险 `601318`、证券 `600030`，以及 ST、停牌、一字涨停不可买、一字跌停不可卖、缺失数据；PR0 不在其中伪造价格或财务数值。
- 每个 case 和两个 JSON 文件都有记录在 `manifest.json` 的 canonical SHA-256；测试除重算外还硬锁 `research-golden-v1` 的顶层与逐 case 哈希，语义变化必须显式提升版本并更新锁值。
- 每个 case 与 dataset 都显式给出 `as_of`、`available_at` 和 `status`，并验证所有 `available_at` 不超过 case 的知识边界。边缘标的使用明确的 `SYNTH-*` 符号，不冒充实时或历史股票状态。

PR0 门禁共同验证无未来数据、缺失值不等于零、Feature Flag 全关兼容以及 canonical hash 稳定性。PR2 落地确定性因子后，必须另行版本化补充可消费的冻结价格/财务输入与预期 factors、snapshot/API 输出，再把该目录升级为真实 Golden canary。

## 验收与回滚

PR0 验收条件：

1. 同一仓库和输入文件重复采集时，除时间字段外的 Git、文件、Schema 与源码哈希一致。
2. 配置内容、Docker 环境变量、数据库业务行和绝对宿主路径不出现在输出中；COPY/static 范围发现敏感文件时在读取内容前失败。
3. 源码和 built static 分轨，均能分别报告缺失、额外和内容不一致。
4. SQLite 采集不产生表、日志、WAL 写入或业务数据变化。
5. 默认 required 核心表完整；缺表只允许作为显式 diagnostic 报告，不能成为生产通过证据；`quick_check` 或外键完整性失败在任何模式下都必须阻断。
6. 正式基线的镜像、Compose 与 runtime 配置身份完整；缺项只允许显式 `--diagnostic`，不能成为生产通过证据。
7. Golden 字段字典、契约 bundle 和 10 个固定 case 的 canonical hash、时间边界全部通过。

本节只读采集工具的回滚只需删除新增脚本、测试、清单和本文档；已生成的一次性发布证据按部署侧留存策略清理。完整 PR0 的迁移、readiness、Docker/Compose 运行态变更必须按[部署与迁移说明](../DEPLOY.md)回滚，不能只删除采集脚本。
