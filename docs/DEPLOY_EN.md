# Deployment Guide

This document explains how to deploy the AI Stock Analysis System to a server.

## Deployment Options Comparison

| Option | Pros | Cons | Recommended For |
|------|------|------|----------|
| **Docker Compose** ⭐ | One-click deploy, isolated environment, easy migration, easy upgrade | Requires Docker installation | **Recommended**: Most scenarios |
| **Direct Deployment** | Simple, no extra dependencies | Environment dependencies, migration difficulties | Temporary testing |
| **Systemd Service** | System-level management, auto-start on boot | Complex configuration | Long-term stable operation |
| **Supervisor** | Process management, auto-restart | Requires additional installation | Multi-process management |

**Conclusion: Docker Compose is recommended for the fastest and most convenient migration!**

---

## Option 1: Docker Compose Deployment (Recommended)

### 1. Install Docker

```bash
# Ubuntu/Debian
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER

# CentOS
sudo yum install -y docker docker-compose
sudo systemctl start docker
sudo systemctl enable docker
```

### 2. Prepare Configuration Files

```bash
# Clone code (or upload code to server)
git clone <your-repo-url> /opt/stock-analyzer
cd /opt/stock-analyzer

# Copy and edit configuration file
cp .env.example .env
vim .env  # Fill in real API Keys and configuration
```

### 3. One-Click Start

```bash
# Build and start
docker-compose -f ./docker/docker-compose.yml up -d

# View logs
docker-compose -f ./docker/docker-compose.yml logs -f

# View running status
docker-compose -f ./docker/docker-compose.yml ps
```

### 3.1 Resource Recommendations

Compose applies per-process limits: 1.25 GiB for `server`, 2.5 GiB/3 CPUs for the durable `worker`, and 1 GiB for `analyzer`. The `analyzer` is the sole Scheduler in durable mode, but it still executes full legacy analysis while the flag is off, so it retains the pre-PR1 1 GiB ceiling and does not regress flag-off deployments. `DURABLE_WORKER_CONCURRENCY` controls durable handler concurrency and defaults to `3`; the legacy path still uses `MAX_WORKERS`.

For a lower-memory host, keep `DURABLE_JOBS_ENABLED=false`, use only the legacy `server`/`analyzer` topology, lower `MAX_WORKERS`, and disable non-essential market review, news expansion, and image reports.

### 3.2 Durable Worker + existing Analyzer (opt in)

Set `DURABLE_JOBS_ENABLED=true` in `.env`. Start the Worker first, then recreate the existing `analyzer` and `server` after it becomes healthy:

```bash
docker compose -f ./docker/docker-compose.yml --profile durable up -d worker
docker compose -f ./docker/docker-compose.yml up -d --force-recreate analyzer server
```

The Worker probe reads the component heartbeat in `provider_health` and only accepts the `DURABLE_WORKER_ID` row while it is `idle`/`busy` and no older than `DURABLE_WORKER_HEALTH_MAX_AGE_SECONDS`. The same `analyzer` executes the legacy path with the flag off and only enqueues with it on. During durable startup it waits for a fresh Worker heartbeat before registering any schedule and exits after `DURABLE_WORKER_STARTUP_TIMEOUT_SECONDS` so the container restart policy can retry. API `server` depends only on a successful migration, not Worker health. Compose defines no second `scheduler` service, so no profile can create two schedule owners.

### 3.3 PR2 Tushare research data (opt in)

Before enabling it, verify `TUSHARE_TOKEN` and set `PERSONAL_RESEARCH_ENABLED=true`, `DURABLE_JOBS_ENABLED=true`, and `TUSHARE_RESEARCH_ENABLED=true`. Defaults are `TUSHARE_GLOBAL_CALLS_PER_MINUTE=450` and `TUSHARE_MAX_INFLIGHT=2`; `TUSHARE_ENDPOINT_LIMITS_JSON` may only impose stricter endpoint limits. Production must have exactly one Durable Worker owning Tushare access, while the API and Analyzer never call Tushare Pro directly. With research enabled, the Worker holds a cross-process `database-name.tushare-owner.lock` next to SQLite; an accidental second Worker fails immediately, normal shutdown releases the lock, and a zero-byte file left after a crash is safe for the next process to reacquire without deletion. Cumulative calls, success/failure, rows, latency, peak in-flight requests, endpoint groups, and error types are coalesced no more than once every five seconds and at collection completion into the `provider/tushare/account` row in `provider_health`. Raw research artifacts live in `data/research/raw/` and are not part of the SQLite backup; production must pair them with the corresponding SQLite backup by following the [research raw archive and forensic restore runbook](operations/research-raw-backup_EN.md).

Each Research run treats `prepared.as_of` as a strict knowledge boundary. The frozen path never supplements current quotes, unversioned external intelligence, current portfolio state, or AkShare auxiliary data; missing inputs remain missing, and a daily-bar date beyond the boundary fails closed. `scripts/fetch_tushare_stock_list.py` remains an offline administrative SDK tool outside the Worker account bucket, so stop the research Worker before using it in production and never run it alongside online collection.

### 3.4 PR3 Research Evidence (opt in)

Apply the additive migration first, then enable the complete dependency chain in order:

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

The Worker builds Evidence over frozen dataset/factor artifacts, persists it in `research_evidence_snapshots`, and binds every consuming task through a JobEvent. The Research Snapshot pins it through `evidence_snapshot_hash`. Live collection invokes the injected SearchService result-list entry point at most once. That high-level call retains the existing provider fallback chain, so it does not mean at most one underlying provider request; no candidate provider may follow a result URL to fetch page content. Historical recovery only replays durable bindings. Search output is stored only as bounded normalized snippets in a SQLite Dataset with `raw_ref=None`; this release does not create a result-page/raw sidecar. The 90-day search/news class applies only if a future release introduces that sidecar. Core SQLite backup covers the Dataset and Evidence tables, while other existing research raw files still require a paired archive under the [research raw archive and forensic restore runbook](operations/research-raw-backup_EN.md).

The read-only list requires at least one of `job_id`, `research_snapshot_hash`, or `stock_code` and uses an opaque cursor over `as_of DESC, id DESC`; detail hashes are 64-character lowercase SHA-256 values. Historical reads remain available after the feature flag is disabled. In Web Run Flow, Research Evidence is collapsed by default and only loads task summaries and claim/citation detail after expansion. Source links are rendered only for `http` / `https` URLs.

Use one completed durable task for a read-only verification. When admin authentication is enabled, add a valid session Cookie to `curl`:

```bash
API_BASE="${API_BASE:-http://127.0.0.1:8000}"
TASK_ID="replace-with-completed-task-id"
curl -fsS --get "$API_BASE/api/v1/research/evidence" \
  --data-urlencode "job_id=$TASK_ID" \
  --data-urlencode "limit=20"
```

To roll back, set `RESEARCH_EVIDENCE_ENABLED=false` first and recreate the Worker, Analyzer, and Server. Do not drop or downgrade the Evidence table and do not delete existing JobEvent/hash bindings. Confirm that the same read-only query still returns historical data before disabling the remaining personal-research flags in reverse order.

### 4. Common Management Commands

```bash
# Stop services
docker-compose -f ./docker/docker-compose.yml down

# Restart services
docker-compose -f ./docker/docker-compose.yml restart

# Redeploy after code update
git pull
docker-compose -f ./docker/docker-compose.yml build --no-cache
docker-compose -f ./docker/docker-compose.yml up -d

# Enter container for debugging
docker-compose -f ./docker/docker-compose.yml exec -u dsa stock-analyzer bash

# Manually run analysis once
docker-compose -f ./docker/docker-compose.yml exec -u dsa stock-analyzer python main.py --no-notify
```

### 5. Data Persistence

Data is automatically saved to host directories:
- `./data/` - Database files
- `./logs/` - Log files
- `./reports/` - Analysis reports

### 6. Permissions

The Docker image startup entrypoint automatically creates and fixes ownership for the mounted `./data`, `./logs`, and `./reports` directories, then drops privileges to the non-root `dsa` user (UID 1000). Normal deployments do not require manual host-side `chown` / `chmod`.

If you explicitly set `--user` / Compose `user:`, or use read-only mounts, rootless Docker, NFS, or another environment that prevents the container from fixing ownership, make sure the actual runtime user can write to these directories.

### 7. Liveness and readiness probes

- `/health`, `/api/health`, and `/api/v1/health` remain compatible liveness endpoints. They only prove that the API process can respond.
- `/api/v1/health/ready` is the traffic readiness endpoint. It inspects migration state without applying migrations, then verifies SQLite read and write access. The write probe inserts a transient migration marker and rolls the transaction back immediately.
- Before Durable Worker is enabled, the worker-heartbeat check is reported as `skipped`. Once a deployment explicitly requires that heartbeat, a missing or stale heartbeat makes readiness return HTTP `503`.

Compose first runs a one-shot `migrator` (`python -m src.migrations --apply`). The legacy topology starts `server` and `analyzer` after migration; the durable profile additionally starts the Worker, and the existing `analyzer` does not register its Scheduler until the database component heartbeat is fresh. The image probe is mode-aware: `--serve`, `--serve-only`, legacy WebUI arguments, or `WEBUI_ENABLED=true` must pass readiness on the configured container `API_PORT` (default `8000`); `python main.py --schedule` and other non-HTTP modes must have a live, non-zombie DSA process. `server` receives `DSA_RUNTIME_SCHEDULER_SUPPRESS_START=true`, so only `analyzer` owns the schedule. A standalone `--serve-only` process without that variable still restores saved scheduling configuration; in durable mode it keeps scheduling stopped when Worker is unavailable without making API readiness depend on Worker.

```bash
curl --fail "http://127.0.0.1:${API_PORT:-8000}/api/v1/health/ready"
```

---

## Option 2: Direct Deployment

### 1. Install Python Environment

```bash
# Install Python 3.10+
sudo apt update
sudo apt install -y python3.10 python3.10-venv python3-pip

# Create virtual environment
python3.10 -m venv /opt/stock-analyzer/venv
source /opt/stock-analyzer/venv/bin/activate
```

### 2. Install Dependencies

```bash
cd /opt/stock-analyzer
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
```

### 3. Configure Environment Variables

```bash
cp .env.example .env
vim .env  # Fill in configuration
```

### 4. Run

```bash
# Single run
python main.py

# Scheduled task mode (foreground)
python main.py --schedule

# Background run (using nohup)
nohup python main.py --schedule > /dev/null 2>&1 &
```

---

## Option 3: Systemd Service

Create systemd service file for auto-start on boot and auto-restart:

### 1. Create Service File

```bash
sudo vim /etc/systemd/system/stock-analyzer.service
```

Contents:
```ini
[Unit]
Description=AI Stock Analysis System
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

### 2. Start Service

```bash
# Reload configuration
sudo systemctl daemon-reload

# Start service
sudo systemctl start stock-analyzer

# Enable auto-start on boot
sudo systemctl enable stock-analyzer

# View status
sudo systemctl status stock-analyzer

# View logs
journalctl -u stock-analyzer -f
```

---

## Configuration Guide

### Required Configuration

| Config Item | Description | How to Get |
|--------|------|----------|
| `ANSPIRE_API_KEYS` / `AIHUBMIX_KEY` / `GEMINI_API_KEY` / `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` | Configure at least one AI model key; Anspire or AIHubMix is recommended first | Provider console |
| `STOCK_LIST` | Watchlist | Comma-separated stock codes |
| Notification channel | Configure at least one, such as WeChat Work, Feishu, Telegram, or email | Notification provider |

### Optional Configuration

| Config Item | Default | Description |
|--------|--------|------|
| `SCHEDULE_ENABLED` | `false` | Enable scheduled tasks |
| `SCHEDULE_TIME` | `18:00` | Daily execution time |
| `MARKET_REVIEW_ENABLED` | `true` | Enable market review |
| `ANSPIRE_API_KEYS` | - | Anspire LLM and news search (recommended) |
| `AIHUBMIX_KEY` | - | AIHubMix one-key multi-model access (recommended) |
| `SERPAPI_API_KEYS` | - | SerpAPI realtime financial news search (recommended) |
| `TAVILY_API_KEYS` | - | Tavily news search (optional) |
| `MINIMAX_API_KEYS` | - | MiniMax search (optional) |

---

## Proxy Configuration

If server is in mainland China, accessing Gemini API requires proxy:

### Docker Method

Edit `docker-compose.yml`:
```yaml
environment:
  - http_proxy=http://your-proxy:port
  - https_proxy=http://your-proxy:port
```

### Direct Deployment Method

Edit top of `main.py`:
```python
os.environ["http_proxy"] = "http://your-proxy:port"
os.environ["https_proxy"] = "http://your-proxy:port"
```

---

## Monitoring & Maintenance

### View Logs

```bash
# Docker method
docker-compose -f ./docker/docker-compose.yml logs -f --tail=100

# Direct deployment
tail -f /opt/stock-analyzer/logs/stock_analysis_*.log
```

### Health Check

```bash
# Check process
ps aux | grep main.py

# Check recent reports
ls -la /opt/stock-analyzer/reports/
```

### Routine Maintenance

```bash
# Clean old logs (keep 7 days)
find /opt/stock-analyzer/logs -mtime +7 -delete

# Clean old reports (keep 30 days)
find /opt/stock-analyzer/reports -mtime +30 -delete
```

---

## FAQ

### 1. Docker build failed

```bash
# Clear cache and rebuild
docker-compose -f ./docker/docker-compose.yml build --no-cache
```

### 2. API access timeout

Check proxy configuration, ensure server can access Gemini API.

### 3. Database locked

```bash
# Stop service then delete lock file
rm /opt/stock-analyzer/data/*.lock
```

### 4. Insufficient memory

The default Compose recommendation is already `1G`. If the container still hits OOM or is killed by the platform, raise the memory limit in `docker-compose.yml`; use `2G+` when running `server + analyzer` together, multi-stock analysis, market review, image reports, or screening:
```yaml
deploy:
  resources:
    limits:
      memory: 1G
    reservations:
      memory: 512M
```

For a constrained `512M` deployment, set `MAX_WORKERS=1`, start only one of `server` or `analyzer`, and reduce non-essential market review, news expansion, and image report tasks.

---

## Quick Migration

Migrate from one server to another:

```bash
# Source server: take a consistent SQLite online backup, then package other files
cd /opt/stock-analyzer
mkdir -p backups
python scripts/sqlite_backup.py backup \
  --database data/stock_analysis.db \
  --output backups/stock-analysis.sqlite
tar --exclude='data/stock_analysis.db' --exclude='data/stock_analysis.db-*' \
  -czvf stock-analyzer-backup.tar.gz .env data/ logs/ reports/ \
  backups/stock-analysis.sqlite backups/stock-analysis.sqlite.manifest.json

# Target server: Deploy
mkdir -p /opt/stock-analyzer
cd /opt/stock-analyzer
git clone <your-repo-url> .
tar -xzvf stock-analyzer-backup.tar.gz
python scripts/sqlite_backup.py restore \
  --backup backups/stock-analysis.sqlite \
  --target data/stock_analysis.db
docker-compose -f ./docker/docker-compose.yml up -d
```

Do not copy an active SQLite main file with `cp` or `tar`; committed WAL data may not yet be merged into that file. See the [SQLite online backup, verification, and restore runbook](operations/sqlite-backup.md) (Chinese) for strict verification, restore drills, and production replacement.

---

## Option 4: GitHub Actions Deployment (Serverless)

**The simplest option!** No server needed, leverages GitHub's free compute resources.

### Advantages
- ✅ **Completely free** (2000 minutes/month)
- ✅ **No server needed**
- ✅ **Auto-scheduled execution**
- ✅ **Zero maintenance cost**

### Limitations
- ⚠️ Stateless (fresh environment each run)
- ⚠️ Scheduled timing may have few minutes delay
- ⚠️ Cannot provide HTTP API

### Deployment Steps

#### 1. Create GitHub Repository

```bash
# Initialize git (if not already)
cd /path/to/daily_stock_analysis
git init
git add .
git commit -m "Initial commit"

# Create GitHub repo and push
# After creating new repo on GitHub web:
git remote add origin https://github.com/your-username/daily_stock_analysis.git
git branch -M main
git push -u origin main
```

#### 2. Configure Secrets (Important!)

Go to repo page → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**

Add these Secrets:

| Secret Name | Description | Required |
|------------|------|------|
| `ANSPIRE_API_KEYS` | Anspire Open API Key (one key for LLM and search) | Recommended |
| `AIHUBMIX_KEY` | AIHubMix API Key (one key for multiple model families) | Recommended |
| `ANTHROPIC_API_KEY` | Anthropic API Key | Optional |
| `GEMINI_API_KEY` | Gemini AI API Key | Optional |
| `OPENAI_API_KEY` | OpenAI-compatible API Key | Optional |
| `WECHAT_WEBHOOK_URL` | WeChat Work Bot Webhook | Optional* |
| `FEISHU_WEBHOOK_URL` | Feishu Bot Webhook | Optional* |
| `TELEGRAM_BOT_TOKEN` | Telegram Bot Token | Optional* |
| `TELEGRAM_CHAT_ID` | Telegram Chat ID | Optional* |
| `TELEGRAM_MESSAGE_THREAD_ID` | Telegram Topic ID | Optional* |
| `EMAIL_SENDER` | Sender email | Optional* |
| `EMAIL_PASSWORD` | Email authorization code | Optional* |
| `SERVERCHAN3_SENDKEY` | ServerChan v3 Sendkey | Optional* |
| `CUSTOM_WEBHOOK_URLS` | Custom Webhook (comma-separated for multiple) | Optional* |
| `STOCK_LIST` | Watchlist, e.g., `600519,300750` | ✅ |
| `SERPAPI_API_KEYS` | SerpAPI Key | Recommended |
| `TAVILY_API_KEYS` | Tavily Search API Key | Optional |
| `BOCHA_API_KEYS` | Bocha Search API Key | Optional |
| `BRAVE_API_KEYS` | Brave Search API Key | Optional |
| `MINIMAX_API_KEYS` | MiniMax Coding Plan Web Search | Optional |
| `TUSHARE_TOKEN` | Tushare Token | Optional |
| `GEMINI_MODEL` | Model name (default gemini-2.0-flash) | Optional |

> *Note: Configure at least one notification channel, multiple channels supported for simultaneous push

#### 3. Verify Workflow File

Ensure `.github/workflows/00-daily-analysis.yml` file exists and is committed:

```bash
git add .github/workflows/00-daily-analysis.yml
git commit -m "Add GitHub Actions workflow"
git push
```

#### 4. Manual Test Run

1. Go to repo page → **Actions** tab
2. Select **"Daily Stock Analysis"** workflow
3. Click **"Run workflow"** button
4. Select run mode:
   - `full` - Full analysis (stocks + market)
   - `market-only` - Market review only
   - `stocks-only` - Stock analysis only
5. Click green **"Run workflow"** button

#### 5. View Execution Logs

- Actions page shows run history
- Click specific run record to view detailed logs
- Analysis reports are saved as Artifacts for 30 days

### Schedule Details

Default configuration: **Monday to Friday, 18:00 Beijing Time** auto-execution

Modify time: Edit cron expression in `.github/workflows/00-daily-analysis.yml`:

```yaml
schedule:
  - cron: '0 10 * * 1-5'  # UTC time, +8 = Beijing time
```

Common cron examples:
| Expression | Description |
|--------|------|
| `'0 10 * * 1-5'` | Mon-Fri 18:00 (Beijing) |
| `'30 7 * * 1-5'` | Mon-Fri 15:30 (Beijing) |
| `'0 10 * * *'` | Daily 18:00 (Beijing) |
| `'0 2 * * 1-5'` | Mon-Fri 10:00 (Beijing) |

### Modify Watchlist

Method 1: Modify repo Secret `STOCK_LIST`

Method 2: Modify code directly then push:
```bash
# Modify .env.example or set default value in code
git commit -am "Update stock list"
git push
```

### FAQ

**Q: Why isn't the scheduled task running?**
A: GitHub Actions scheduled tasks may have 5-15 minute delays, and only trigger when repo has activity. Long periods without commits may cause workflow to be disabled.

**Q: How to view historical reports?**
A: Actions → Select run record → Artifacts → Download `analysis-reports-xxx`

**Q: Is the free quota enough?**
A: Each run takes about 2-5 minutes, 22 workdays per month = 44-110 minutes, well below the 2000 minute limit.

---

**Wishing you a smooth deployment!**
