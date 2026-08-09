# 🚀 部署指南

本文档介绍如何将 A股自选股智能分析系统部署到服务器。

## 📋 部署方案对比

| 方案 | 优点 | 缺点 | 推荐场景 |
|------|------|------|----------|
| **Docker Compose** ⭐ | 一键部署、环境隔离、易迁移、易升级 | 需要安装 Docker | **推荐**：大多数场景 |
| **直接部署** | 简单直接、无额外依赖 | 环境依赖、迁移麻烦 | 临时测试 |
| **Systemd 服务** | 系统级管理、开机自启 | 配置繁琐 | 长期稳定运行 |
| **Supervisor** | 进程管理、自动重启 | 需要额外安装 | 多进程管理 |

**结论：推荐使用 Docker Compose，迁移最快最方便！**

---

## 🐳 方案一：Docker Compose 部署（推荐）

### 1. 安装 Docker

```bash
# Ubuntu/Debian
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER

# CentOS
sudo yum install -y docker docker-compose
sudo systemctl start docker
sudo systemctl enable docker
```

### 2. 准备配置文件

```bash
# 克隆代码（或上传代码到服务器）
git clone <your-repo-url> /opt/stock-analyzer
cd /opt/stock-analyzer

# 复制并编辑配置文件
cp .env.example .env
vim .env  # 填入真实的 API Key 等配置
```

### 3. 一键启动

```bash
# 构建并启动（同时包含定时分析和 Web 界面服务）
docker-compose -f ./docker/docker-compose.yml up -d

# 查看日志
docker-compose -f ./docker/docker-compose.yml logs -f

# 查看运行状态
docker-compose -f ./docker/docker-compose.yml ps
```

启动成功后，在浏览器输入 `http://服务器公网IP:8000` 即可打开 Web 管理界面。如果打不开，记得先在云服务器控制台的「安全组」里放行 8000 端口。

> 不知道怎么访问？→ [云服务器 Web 界面访问指南](deploy-webui-cloud.md)

### 3.1 资源建议

默认 Compose 对进程分别限额：`server` 1.25 GiB、durable `worker` 2.5 GiB/3 CPU、`analyzer` 1 GiB。`analyzer` 在 durable 模式是唯一 Scheduler，但在开关关闭时仍执行完整 legacy 分析，因此保留二开前的 1 GiB 上限，避免 Feature Flag 全关时出现资源回归。Worker 的任务并发由 `DURABLE_WORKER_CONCURRENCY` 控制，默认 `3`；旧路径仍使用 `MAX_WORKERS`。

如果只能使用较低内存，请保持 `DURABLE_JOBS_ENABLED=false` 并只启动 legacy `server`/`analyzer` 拓扑，降低 `MAX_WORKERS`，同时关闭非必要的大盘复盘、新闻扩展和图片报告能力。

### 3.2 Durable Worker + 现有 Analyzer（按需启用）

先在 `.env` 设置 `DURABLE_JOBS_ENABLED=true`。切换时先启动 Worker，待其健康后再重建现有 `analyzer` 与 `server`：

```bash
docker compose -f ./docker/docker-compose.yml --profile durable up -d worker
docker compose -f ./docker/docker-compose.yml up -d --force-recreate analyzer server
```

Worker 健康检查只读 `provider_health` 中的组件心跳，要求 `DURABLE_WORKER_ID` 对应的心跳处于 `idle`/`busy` 且不超过 `DURABLE_WORKER_HEALTH_MAX_AGE_SECONDS`。同一个 `analyzer` 在开关关闭时执行旧分析，在开启时仅入队；durable 启动时它会在注册任何定时任务前等待新鲜 Worker 心跳，超过 `DURABLE_WORKER_STARTUP_TIMEOUT_SECONDS` 后退出并由容器策略重启。API `server` 只依赖迁移成功，不依赖 Worker 健康。Compose 中不存在第二个 `scheduler` 服务，因此任何 profile 都不会产生两个调度 owner。

### 3.3 PR2 Tushare 研究数据（按需启用）

启用前必须确认 `TUSHARE_TOKEN` 有效，并保持 `PERSONAL_RESEARCH_ENABLED=true`、`DURABLE_JOBS_ENABLED=true`、`TUSHARE_RESEARCH_ENABLED=true`。默认 `TUSHARE_GLOBAL_CALLS_PER_MINUTE=450`、`TUSHARE_MAX_INFLIGHT=2`；`TUSHARE_ENDPOINT_LIMITS_JSON` 只能设置更严格的端点上限。生产只允许一个 Durable Worker 持有 Tushare 调用权，API 与 Analyzer 不直接调用 Tushare Pro。启用研究后，Worker 会在 SQLite 同目录持有 `数据库文件名.tushare-owner.lock` 跨进程独占锁；意外启动第二个 Worker 会立即失败，进程正常退出后释放锁，崩溃留下的 0 字节文件无需删除且可由下一进程重新获取。Provider 的累计调用、成功/失败、行数、延迟、峰值在途数、端点和错误类型会以不高于每 5 秒一次并在采集结束时合并写入 `provider_health` 的 `provider/tushare/account` 行。原始研究文件位于 `data/research/raw/`，它不属于 SQLite 备份；生产必须按[研究原始数据归档与取证恢复](operations/research-raw-backup.md)将它与对应 SQLite 备份成对归档、校验并演练恢复。

Research 运行以本轮 `prepared.as_of` 为严格知识边界；冻结路径不会补入当前实时报价、未版本化外部资讯、当前组合状态或 AkShare 辅助数据。缺失数据保持缺失，日线日期晚于边界会直接拒绝冻结。`scripts/fetch_tushare_stock_list.py` 仅是离线管理员工具，不受 Worker 账号桶保护，生产使用时必须先停止研究 Worker且不得与在线采集并行。

### 3.4 PR3 Research Evidence（按需启用）

先应用追加式迁移，再按依赖顺序开启完整链路：

```bash
python -m src.migrations --apply
# .env
PERSONAL_RESEARCH_ENABLED=true
DURABLE_JOBS_ENABLED=true
TUSHARE_RESEARCH_ENABLED=true
RESEARCH_FACTORS_ENABLED=true
RESEARCH_EVIDENCE_ENABLED=true

docker compose -f ./docker/docker-compose.yml --profile durable up -d --force-recreate worker
docker compose -f ./docker/docker-compose.yml up -d --force-recreate analyzer server
```

Evidence 由 Worker 在已冻结 dataset / factor 之上构造，持久化到 `research_evidence_snapshots`，并通过 JobEvent 绑定到实际消费它的 task；Research Snapshot 通过 `evidence_snapshot_hash` 固定引用。在线阶段最多调用一次注入的 SearchService 结果列表入口；该高层调用保留既有 provider fallback，因此不等同于最多一次底层 provider 请求，但任何候选 provider 都不得跟随结果 URL 抓正文。历史恢复只重放已绑定数据，不重新搜索。搜索结果只以 bounded normalized snippet 写入 SQLite Dataset，`raw_ref=None`，本版不创建 result-page/raw sidecar；未来若引入该 sidecar，才适用搜索/新闻 90 天保留分类。SQLite 核心备份覆盖 Dataset 与 Evidence 表，现有其它研究 raw 文件仍需按[研究原始数据归档与取证恢复](operations/research-raw-backup.md)单独成对归档。

只读列表至少需要 `job_id`、`research_snapshot_hash`、`stock_code` 之一，按 `as_of DESC, id DESC` 使用 opaque cursor 分页；详情 hash 必须是 64 位小写 SHA-256。功能开关关闭后历史仍可读。Web Run Flow 中的研究证据默认折叠，展开时才按当前 Task 加载摘要与 claim/citation；来源链接只允许 `http` / `https`。

可用一个已完成的 durable task 做只读验证；启用管理员认证时给 `curl` 补充有效的 session Cookie：

```bash
API_BASE="${API_BASE:-http://127.0.0.1:8000}"
TASK_ID="replace-with-completed-task-id"
curl -fsS --get "$API_BASE/api/v1/research/evidence" \
  --data-urlencode "job_id=$TASK_ID" \
  --data-urlencode "limit=20"
```

回滚只需先设置 `RESEARCH_EVIDENCE_ENABLED=false`，再重建 Worker、Analyzer 与 Server；不要删除或降级 Evidence 表，也不要清理已有 JobEvent / hash 绑定。关闭开关后用同一只读请求确认历史仍可查询，再按相反顺序关闭其它个人投研开关。

### 3.5 PR4 Bounded Research Debate（按需启用）

先应用追加式迁移，并保持 PR3 的完整依赖链开启，再启用 Debate：

```bash
python -m src.migrations --apply
# .env
PERSONAL_RESEARCH_ENABLED=true
DURABLE_JOBS_ENABLED=true
TUSHARE_RESEARCH_ENABLED=true
RESEARCH_FACTORS_ENABLED=true
RESEARCH_EVIDENCE_ENABLED=true
RESEARCH_DEBATE_ENABLED=true

docker compose -f ./docker/docker-compose.yml --profile durable up -d --force-recreate worker
docker compose -f ./docker/docker-compose.yml up -d --force-recreate analyzer server
```

有可引用 Evidence 时，Worker 从同一份冻结 Evidence 分别执行 Bull、Bear 两个独立、纯文本且有界的高层 completion；每次 durable attempt 对每个尚未解析的 stance 至多发起一次，已经持久化的成功或终止失败 stance 不会重调。provider 返回到检查点提交之间的崩溃窗口可能让尚未持久化的 stance 在 retry 中再次调用，因此这里不承诺整个 job 生命周期的物理请求 exactly-once。没有可引用 Evidence 时不调用模型。Debate 不调用工具、网络或记忆，不抓取新资料，也不生成 arbiter、thesis、交易动作、目标价或仓位建议。每侧最多 6 条 argument 和 6 条 open question；argument 只能引用冻结 Evidence 中已有的 claim/citation ID，不能把模型解释提升为新证据。一个 stance 的终止失败可形成 `partial`，两侧均失败形成 `generation_failed`；瞬时失败交回 durable retry，已经持久化且重新校验通过的 turn 可在 lease reclaim 后复用，避免重复调用。

不可变 request、turn、snapshot 分别写入 `research_debate_requests`、`research_debate_turns`、`research_debate_snapshots`。所有写入与 JobEvent 绑定都受当前 durable lease 和取消状态 fence；失效或已取消 lease 不能提交产物。成功 stance 用 `research_debate_turn` 绑定，终止失败 stance 用仅含安全错误码的 `research_debate_failure` 绑定；重试只补未解析 stance，且 prompt/route 漂移会 fail closed。`research_debate_snapshot` JobEvent 记录实际消费任务，`research_snapshots.debate_snapshot_hash` 固定本轮 Debate，并同时保留 `evidence_snapshot_hash` lineage。request 的精确 messages 只属于持久化执行记录，任何 Debate API 都不得返回。

只读接口不依赖 `RESEARCH_DEBATE_ENABLED`，所以关闭写入开关后历史仍可读：

- `GET /api/v1/research/debates`：至少提供 `job_id`、`research_snapshot_hash`、`stock_code` 之一；可选 `evidence_snapshot_hash`、带 UTC offset 的 `as_of`、opaque `cursor` 和 `limit`（默认 20、最大 100）。结果按 `as_of DESC, id DESC` 稳定分页，只返回 status、argument/open-question count、hash 与 lineage 摘要，不返回完整 Debate。
- `GET /api/v1/research/debates/{debate_hash}`：按 64 位小写 SHA-256 返回严格 typed 的 bounded payload；包含 Bull/Bear turn、失败码和限制，但不包含 request messages。

Web Run Flow 的“研究辩论”位于“研究证据”下方，默认折叠，只在当前来源是 Task 且用户展开时加载分页摘要，再按 hash 懒加载详情。列表、详情和加载更多均可重试；切换 Task 会丢弃旧请求结果。所有模型文本按纯文本渲染，不生成可点击链接或执行 HTML。

可用一个已完成的 durable task 验证列表和详情；启用管理员认证时给 `curl` 补充有效 session Cookie：

```bash
API_BASE="${API_BASE:-http://127.0.0.1:8000}"
TASK_ID="replace-with-completed-task-id"
curl -fsS --get "$API_BASE/api/v1/research/debates" \
  --data-urlencode "job_id=$TASK_ID" \
  --data-urlencode "limit=20"
curl -fsS "$API_BASE/api/v1/research/debates/replace-with-64-char-lowercase-hash"
```

回滚时先设置 `RESEARCH_DEBATE_ENABLED=false`，再重建 Worker、Analyzer 与 Server。不要删除或降级三张 Debate 表，也不要清理 request/turn/snapshot、JobEvent 或 Research Snapshot hash 绑定；用相同只读请求确认历史仍可查询后，再按相反顺序关闭上游开关。

### 4. 常用管理命令

```bash
# 停止服务
docker-compose -f ./docker/docker-compose.yml down

# 重启服务
docker-compose -f ./docker/docker-compose.yml restart

# 更新代码后重新部署
git pull
docker-compose -f ./docker/docker-compose.yml build --no-cache
docker-compose -f ./docker/docker-compose.yml up -d

# 进入容器调试
docker-compose -f ./docker/docker-compose.yml exec -u dsa stock-analyzer bash

# 手动执行一次分析
docker-compose -f ./docker/docker-compose.yml exec -u dsa stock-analyzer python main.py --no-notify
```

### 5. 数据持久化

数据自动保存在宿主机目录：
- `./data/` - 数据库文件
- `./logs/` - 日志文件
- `./reports/` - 分析报告

### 6. 权限说明

Docker 镜像启动入口会自动创建并修复 `./data`、`./logs`、`./reports` 对应挂载目录的权限，然后降权为非 root 用户 (`dsa`, UID 1000) 运行应用。普通部署不需要手动 `chown` / `chmod`。

如果你显式指定了 `--user` / Compose `user:`，或使用只读挂载、rootless Docker、NFS 等不允许容器修复属主的环境，请确保实际运行用户对这些目录具备写入权限。

### 7. 健康检查与就绪检查

- `/health`、`/api/health`、`/api/v1/health` 是兼容的 liveness 接口，只表示 API 进程仍能响应。
- `/api/v1/health/ready` 是接流量前的 readiness 接口，会只读检查迁移版本，并验证 SQLite 可读及可写。写探针插入临时 migration marker 后立即回滚，不保留业务数据。
- Durable Worker 尚未启用时，Worker 心跳项显示为 `skipped`；后续显式要求 Worker 心跳后，心跳失败会令 readiness 返回 HTTP `503`。

Compose 会先运行一次性 `migrator`（`python -m src.migrations --apply`）。legacy 拓扑只在迁移成功后启动 `server` 和 `analyzer`；durable profile 额外启动 Worker，现有 `analyzer` 自身等待数据库组件心跳新鲜后才注册 Scheduler。镜像内的模式感知探针会检查实际 DSA 进程：`--serve`、`--serve-only`、旧版 WebUI 参数或 `WEBUI_ENABLED=true` 必须使用容器内 `API_PORT`（默认 `8000`）通过 readiness；默认 `python main.py --schedule` 等非 HTTP 模式必须存在仍存活且非僵尸的 DSA 进程。`server` 注入 `DSA_RUNTIME_SCHEDULER_SUPPRESS_START=true`，因此只有 `analyzer` 持有调度权。独立启动、未设置该变量的 `--serve-only` 仍会恢复已保存的调度配置；durable 模式下如 Worker 尚未就绪，该进程会保持 Scheduler 停止，但不会让 API readiness 依赖 Worker。

```bash
curl --fail "http://127.0.0.1:${API_PORT:-8000}/api/v1/health/ready"
```

---

## 🖥️ 方案二：直接部署

### 1. 安装 Python 环境

```bash
# 安装 Python 3.10+
sudo apt update
sudo apt install -y python3.10 python3.10-venv python3-pip

# 创建虚拟环境
python3.10 -m venv /opt/stock-analyzer/venv
source /opt/stock-analyzer/venv/bin/activate
```

### 2. 安装依赖

```bash
cd /opt/stock-analyzer
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
```

### 3. 配置环境变量

```bash
cp .env.example .env
vim .env  # 填入配置
```

### 4. 运行

```bash
# 单次运行
python main.py

# 定时任务模式（前台运行）
python main.py --schedule

# 后台运行（使用 nohup）
nohup python main.py --schedule > /dev/null 2>&1 &

# 启动 Web 管理界面（云服务器需先在 .env 中设置 WEBUI_HOST=0.0.0.0）
python main.py --webui-only

# 启动 Web 界面（启动时执行一次分析；需每日定时请加 --schedule 或设 SCHEDULE_ENABLED=true）
python main.py --webui
```

> 不知道怎么访问？→ [云服务器 Web 界面访问指南](deploy-webui-cloud.md)

---

## 🔧 方案三：Systemd 服务

创建 systemd 服务文件实现开机自启和自动重启：

### 1. 创建服务文件

```bash
sudo vim /etc/systemd/system/stock-analyzer.service
```

内容：
```ini
[Unit]
Description=A股自选股智能分析系统
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/stock-analyzer
Environment="PATH=/opt/stock-analyzer/venv/bin"
ExecStart=/opt/stock-analyzer/venv/bin/python main.py --schedule
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```

### 2. 启动服务

```bash
# 重载配置
sudo systemctl daemon-reload

# 启动服务
sudo systemctl start stock-analyzer

# 开机自启
sudo systemctl enable stock-analyzer

# 查看状态
sudo systemctl status stock-analyzer

# 查看日志
journalctl -u stock-analyzer -f
```

---

## ⚙️ 配置说明

### 必须配置项

| 配置项 | 说明 | 获取方式 |
|--------|------|----------|
| `ANSPIRE_API_KEYS` / `AIHUBMIX_KEY` / `GEMINI_API_KEY` / `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` | AI 模型至少配置一个；推荐优先 Anspire 或 AIHubMix | 对应服务商控制台 |
| `STOCK_LIST` | 自选股列表 | 逗号分隔的股票代码 |
| 通知渠道 | 至少配置一个，如企业微信、飞书、Telegram 或邮件 | 对应通知平台 |

### 可选配置项

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `SCHEDULE_ENABLED` | `false` | 是否启用定时任务 |
| `SCHEDULE_TIME` | `18:00` | 每日执行时间 |
| `MARKET_REVIEW_ENABLED` | `true` | 是否启用大盘复盘 |
| `ANSPIRE_API_KEYS` | - | Anspire 大模型与新闻搜索（推荐） |
| `AIHUBMIX_KEY` | - | AIHubMix 一 Key 多模型（推荐） |
| `SERPAPI_API_KEYS` | - | SerpAPI 实时金融新闻搜索（推荐） |
| `TAVILY_API_KEYS` | - | Tavily 新闻搜索（可选） |
| `MINIMAX_API_KEYS` | - | MiniMax 搜索（可选） |

---

## 🌐 代理配置

如果服务器在国内，访问 Gemini API 需要代理：

### Docker 方式

编辑 `docker-compose.yml`：
```yaml
environment:
  - http_proxy=http://your-proxy:port
  - https_proxy=http://your-proxy:port
```

### 直接部署方式

编辑 `main.py` 顶部：
```python
os.environ["http_proxy"] = "http://your-proxy:port"
os.environ["https_proxy"] = "http://your-proxy:port"
```

---

## 📊 监控与维护

### 日志查看

```bash
# Docker 方式
docker-compose -f ./docker/docker-compose.yml logs -f --tail=100

# 直接部署
tail -f /opt/stock-analyzer/logs/stock_analysis_*.log
```

### 健康检查

```bash
# 检查进程
ps aux | grep main.py

# 检查最近的报告
ls -la /opt/stock-analyzer/reports/
```

### 定期维护

```bash
# 清理旧日志（保留7天）
find /opt/stock-analyzer/logs -mtime +7 -delete

# 清理旧报告（保留30天）
find /opt/stock-analyzer/reports -mtime +30 -delete
```

---

## ❓ 常见问题

### 1. Docker 构建失败

```bash
# 清理缓存重新构建
docker-compose -f ./docker/docker-compose.yml build --no-cache
```

### 2. API 访问超时

检查代理配置，确保服务器能访问 Gemini API。

### 3. 数据库锁定

```bash
# 停止服务后删除 lock 文件
rm /opt/stock-analyzer/data/*.lock
```

### 4. 内存不足

默认 Compose 已推荐 `1G`。如果仍出现 OOM 或平台杀掉容器，请提高 `docker-compose.yml` 中的内存限制；同时跑 `server + analyzer`、多股票、大盘复盘、图片报告或选股时建议 `2G+`：
```yaml
deploy:
  resources:
    limits:
      memory: 1G
    reservations:
      memory: 512M
```

低配环境只能使用 `512M` 时，建议设置 `MAX_WORKERS=1`，只启动 `server` 或 `analyzer` 其中一个服务，并减少非必要的大盘复盘、新闻扩展和图片报告任务。

### 5. WebUI 打开后 UI 元素异常变大 / 布局错乱

**症状**：能访问 8000 端口，但页面上的文字、按钮、卡片异常放大，没有正常布局。

**根因**：`static/index.html` 存在，但 CSS/JS 资源文件缺失（`static/assets/` 为空或不存在），浏览器无法加载样式与脚本，导致裸 HTML 渲染。

**解决方法**：

- **Docker 部署**：执行以下命令重新构建镜像（确保前端已正确打包进镜像）：
  ```bash
  docker-compose -f ./docker/docker-compose.yml down
  docker-compose -f ./docker/docker-compose.yml build --no-cache
  docker-compose -f ./docker/docker-compose.yml up -d
  ```
  构建完成后刷新浏览器缓存（`Ctrl+Shift+R`）再访问。

- **直接部署（pip + python）**：先构建前端，再启动服务：
  ```bash
  # 安装 Node.js 18+（推荐 20+，如尚未安装）
  # 构建前端
  cd apps/dsa-web
  npm ci
  npm run build
  cd ../..
  # 启动服务
  python main.py --webui-only
  ```

**验证**：用浏览器开发者工具（F12 → Network）检查是否有 `/assets/index-*.js` 和 `/assets/index-*.css` 的 404 错误；如有，说明资源缺失，按上述步骤重新构建即可。

---

## 🔄 快速迁移

从一台服务器迁移到另一台：

```bash
# 源服务器：先做一致性 SQLite 在线备份，再打包其它文件
cd /opt/stock-analyzer
mkdir -p backups
python scripts/sqlite_backup.py backup \
  --database data/stock_analysis.db \
  --output backups/stock-analysis.sqlite
tar --exclude='data/stock_analysis.db' --exclude='data/stock_analysis.db-*' \
  -czvf stock-analyzer-backup.tar.gz .env data/ logs/ reports/ \
  backups/stock-analysis.sqlite backups/stock-analysis.sqlite.manifest.json

# 目标服务器：部署
mkdir -p /opt/stock-analyzer
cd /opt/stock-analyzer
git clone <your-repo-url> .
tar -xzvf stock-analyzer-backup.tar.gz
python scripts/sqlite_backup.py restore \
  --backup backups/stock-analysis.sqlite \
  --target data/stock_analysis.db
docker-compose -f ./docker/docker-compose.yml up -d
```

不要用 `cp` 或 `tar` 直接复制活动的 SQLite 主文件；WAL 中已提交的数据可能尚未合并到主文件。校验、恢复演练和生产替换的完整流程见 [SQLite 在线备份、校验与恢复](operations/sqlite-backup.md)。

---

## ☁️ 方案四：GitHub Actions 部署（免服务器）

**最简单的方案！** 无需服务器，利用 GitHub 免费计算资源。

### 优势
- ✅ **完全免费**（每月 2000 分钟）
- ✅ **无需服务器**
- ✅ **自动定时执行**
- ✅ **零维护成本**

### 限制
- ⚠️ 无状态（每次运行是新环境）
- ⚠️ 定时可能有几分钟延迟
- ⚠️ 无法提供 HTTP API

### 部署步骤

#### 1. 创建 GitHub 仓库

```bash
# 初始化 git（如果还没有）
cd /path/to/daily_stock_analysis
git init
git add .
git commit -m "Initial commit"

# 创建 GitHub 仓库并推送
# 在 GitHub 网页上创建新仓库后：
git remote add origin https://github.com/你的用户名/daily_stock_analysis.git
git branch -M main
git push -u origin main
```

#### 2. 配置 Secrets（重要！）

打开仓库页面 → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**

添加以下 Secrets：

| Secret 名称 | 说明 | 必填 |
|------------|------|------|
| `ANSPIRE_API_KEYS` | Anspire Open API Key（一 Key 启用大模型与搜索） | 推荐 |
| `AIHUBMIX_KEY` | AIHubMix API Key（一 Key 多模型） | 推荐 |
| `ANTHROPIC_API_KEY` | Anthropic API Key | 可选 |
| `GEMINI_API_KEY` | Gemini AI API Key | 可选 |
| `OPENAI_API_KEY` | OpenAI 兼容 API Key | 可选 |
| `WECHAT_WEBHOOK_URL` | 企业微信机器人 Webhook | 可选* |
| `FEISHU_WEBHOOK_URL` | 飞书机器人 Webhook | 可选* |
| `TELEGRAM_BOT_TOKEN` | Telegram Bot Token | 可选* |
| `TELEGRAM_CHAT_ID` | Telegram Chat ID | 可选* |
| `TELEGRAM_MESSAGE_THREAD_ID` | Telegram Topic ID | 可选* |
| `EMAIL_SENDER` | 发件人邮箱 | 可选* |
| `EMAIL_PASSWORD` | 邮箱授权码 | 可选* |
| `SERVERCHAN3_SENDKEY` | Server酱³ Sendkey | 可选* |
| `CUSTOM_WEBHOOK_URLS` | 自定义 Webhook（多个逗号分隔） | 可选* |
| `STOCK_LIST` | 自选股列表，如 `600519,300750` | ✅ |
| `SERPAPI_API_KEYS` | SerpAPI Key | 推荐 |
| `TAVILY_API_KEYS` | Tavily 搜索 API Key | 可选 |
| `BOCHA_API_KEYS` | 博查搜索 API Key | 可选 |
| `BRAVE_API_KEYS` | Brave Search API Key | 可选 |
| `MINIMAX_API_KEYS` | MiniMax Coding Plan Web Search | 可选 |
| `SEARXNG_BASE_URLS` | SearXNG 自建实例（无配额兜底，需在 settings.yml 启用 format: json）；留空时默认自动发现公共实例 | 可选 |
| `SEARXNG_PUBLIC_INSTANCES_ENABLED` | 是否在 `SEARXNG_BASE_URLS` 为空时自动从 `searx.space` 获取公共实例（默认 `true`） | 可选 |
| `TUSHARE_TOKEN` | Tushare Token | 可选 |
| `GEMINI_MODEL` | 模型名称（默认 gemini-2.0-flash） | 可选 |

> *注：通知渠道至少配置一个，支持多渠道同时推送

#### 3. 验证 Workflow 文件

确保 `.github/workflows/00-daily-analysis.yml` 文件存在且已提交：

```bash
git add .github/workflows/00-daily-analysis.yml
git commit -m "Add GitHub Actions workflow"
git push
```

#### 4. 手动测试运行

1. 打开仓库页面 → **Actions** 标签
2. 选择 **"每日股票分析"** workflow
3. 点击 **"Run workflow"** 按钮
4. 选择运行模式：
   - `full` - 完整分析（股票+大盘）
   - `market-only` - 仅大盘复盘
   - `stocks-only` - 仅股票分析
5. 点击绿色 **"Run workflow"** 按钮

#### 5. 查看执行日志

- Actions 页面可以看到运行历史
- 点击具体的运行记录查看详细日志
- 分析报告会作为 Artifact 保存 30 天

### 定时说明

默认配置：**周一到周五，北京时间 18:00** 自动执行

修改时间：编辑 `.github/workflows/00-daily-analysis.yml` 中的 cron 表达式：

```yaml
schedule:
  - cron: '0 10 * * 1-5'  # UTC 时间，+8 = 北京时间
```

常用 cron 示例：
| 表达式 | 说明 |
|--------|------|
| `'0 10 * * 1-5'` | 周一到周五 18:00（北京时间） |
| `'30 7 * * 1-5'` | 周一到周五 15:30（北京时间） |
| `'0 10 * * *'` | 每天 18:00（北京时间） |
| `'0 2 * * 1-5'` | 周一到周五 10:00（北京时间） |

### 修改自选股

方法一：修改仓库 Secret `STOCK_LIST`

方法二：直接修改代码后推送：
```bash
# 修改 .env.example 或在代码中设置默认值
git commit -am "Update stock list"
git push
```

### 常见问题

**Q: 为什么定时任务没有执行？**
A: GitHub Actions 定时任务可能有 5-15 分钟延迟，且仅在仓库有活动时才触发。长时间无 commit 可能导致 workflow 被禁用。

**Q: 如何查看历史报告？**
A: Actions → 选择运行记录 → Artifacts → 下载 `analysis-reports-xxx`

**Q: 免费额度够用吗？**
A: 每次运行约 2-5 分钟，一个月 22 个工作日 = 44-110 分钟，远低于 2000 分钟限制。

---

## 🌐 云服务器上部署了，但不知道怎么用浏览器访问？

详见 → [云服务器 Web 界面访问指南](deploy-webui-cloud.md)

涵盖：直接部署和 Docker 两种方式的启动与访问、安全组/防火墙配置、常见问题排查、Nginx 反向代理（可选）。

---

**祝部署顺利！🎉**
