# Research Raw Forensic Archive, Verification, And Isolated Restore

`scripts/research_raw_archive.py` binds a completed SQLite online backup to the complete content-addressed
`research/raw/` tree. Use this bundle when full forensic recovery must retain original provider responses.
Ordinary business recovery can still rely on `normalized_json`, Factor Snapshots, and Research Snapshots in SQLite.

A forensic bundle consists of all three items:

- a verified, standalone SQLite backup created by `scripts/sqlite_backup.py`;
- an uncompressed tar archive whose members remain the gzip files written by `RawArtifactStore`;
- a raw manifest containing the SQLite SHA-256, archive SHA-256, complete file paths, compressed SHA-256 values,
  decompressed content SHA-256 values, compressed/uncompressed sizes, and the database reference set.

The manifest canonical SHA-256 detects accidental edits; it is not a digital signature. Store the database backup,
raw archive, and manifest in read-only or immutable backup storage, and record or sign their digests externally.

## Create A Complete Forensic Bundle

Stop the Durable Worker and quiesce API, Scheduler, Bot, and every process that can write SQLite or `research/raw/`.
Create the SQLite backup first, then create the raw archive while writes remain stopped:

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

Successful creation publishes both `research-raw-20260808.tar` and
`research-raw-20260808.tar.manifest.json`. The archive and manifest must have the same existing parent directory and
both targets must be absent. Temporary files are written and fsynced first, then published without replacement. If
the manifest publication fails, the archive published by that run is rolled back and temporary files are cleaned.
The output files and SQLite backup must be outside the raw root.

`create` enumerates the entire raw root, not only currently referenced artifacts. It accepts only
`<first-two-sha256>/<sha256>.<lowercase-extension>.gz` and rejects symlinks, junctions, nested directories, retention
staging, `.tmp`, and every other noncanonical entry. Each gzip is actually decompressed; its content SHA-256 must
match its shard and filename. Every non-empty `research_dataset_snapshots.raw_ref_json` in the SQLite backup must
identify the exact archived path, hash, and sizes. Canonical but currently unreferenced files are archived as well.

This is a write-quiesced backup command, not an online snapshotter. It detects ordinary additions, removals, and
rewrites during enumeration, but that check does not replace stopping all writers.

## Strict Verification

Supply the bound SQLite backup after every copy, upload, download, and before and after recovery:

```bash
python scripts/sqlite_backup.py verify \
  --backup /srv/dsa/backups/stock-analysis-20260808.sqlite

python scripts/research_raw_archive.py verify \
  --archive /srv/dsa/backups/research-raw-20260808.tar \
  --database-backup /srv/dsa/backups/stock-analysis-20260808.sqlite
```

Raw verification recalculates archive, SQLite, and manifest identities; runs SQLite `quick_check`; reloads the raw
references; and checks each tar member path, type, metadata, compressed hash, decompressed hash, and size. Truncation,
edits, traversal, duplicate or extra members, non-regular members, broken gzip data, a missing SQLite reference,
SQLite sidecars, or a database reference set different from the manifest all fail closed.

Pass `--manifest /path/to/raw.manifest.json` when using a non-default manifest location. Renaming the archive fails
its bound filename identity. A restored SQLite copy may have a different filename, but its SHA-256, byte size, and
raw reference set must still exactly match the manifest. Do not hand-edit the manifest to bypass verification.

## Complete SQLite + Raw Isolated Restore Order

Always restore into new isolated paths. Create the two parent directories first; the target database file and target
raw directory must not exist:

```bash
mkdir -p /srv/dsa/restore-drill/data/research

# 1. Strictly verify the complete bundle
python scripts/sqlite_backup.py verify \
  --backup /srv/dsa/backups/stock-analysis-20260808.sqlite
python scripts/research_raw_archive.py verify \
  --archive /srv/dsa/backups/research-raw-20260808.tar \
  --database-backup /srv/dsa/backups/stock-analysis-20260808.sqlite

# 2. Restore the bound SQLite backup
python scripts/sqlite_backup.py restore \
  --backup /srv/dsa/backups/stock-analysis-20260808.sqlite \
  --target /srv/dsa/restore-drill/data/stock_analysis.db

# 3. Restore the bound complete raw root
python scripts/research_raw_archive.py restore \
  --archive /srv/dsa/backups/research-raw-20260808.tar \
  --database-backup /srv/dsa/backups/stock-analysis-20260808.sqlite \
  --target-root /srv/dsa/restore-drill/data/research/raw

# 4. Reverify restored bytes, then run read-only smoke checks with the candidate image
python scripts/research_raw_archive.py verify \
  --archive /srv/dsa/backups/research-raw-20260808.tar \
  --database-backup /srv/dsa/restore-drill/data/stock_analysis.db
```

Step 4 succeeds only when isolated SQLite is byte-identical to the backup, as guaranteed by
`scripts/sqlite_backup.py restore`. Next, use an isolated configuration to check Research Dataset/Snapshot queries,
historical reports, Portfolio, Decision Signal, and Outcome read paths, and sample database references through
`RawArtifactStore.read`.

Raw `restore` never replaces an existing directory and has no production override. It constructs and validates a
staging tree in the target parent, atomically claims an absent target with `mkdir(exist_ok=False)`, and then publishes
the shards. A failure cleans the run's staging and partially claimed target. A concurrently created target is neither
overwritten nor deleted.

On a new disaster-recovery host, the absent canonical raw path may be the direct `--target-root`. On an existing
production host, complete isolated restore and read-only acceptance first, then follow the stopped-service database
rules in [SQLite Online Backup, Verification, And Restore](sqlite-backup.md). For raw cutover, keep all services
stopped, preserve the old raw directory under a timestamped rollback name on the same filesystem, and have operations
atomically rename the accepted isolated directory to canonical `research/raw/`. This tool intentionally does not
perform that production replacement or delete the old tree. Never mix SQLite and raw artifacts from different
manifest-bound bundles.

## 15-Minute RTO Objective And Drills

The objective is **RTO no greater than 900 seconds**. Its measured interval starts when the backup artifacts are
already locally available on the recovery host, and covers strict verification, isolated SQLite restore, isolated
raw restore, and the required read-only smoke checks until the candidate data set is ready for cutover. Incident
detection, approvals, write quiescence, off-site download/network transfer, new-host provisioning, and DNS/traffic
cutover are measured separately under the higher-level disaster-recovery SLO.

Every successful `create`, `verify`, and `restore` command emits JSON `elapsed_seconds`. The repository test performs
the complete flow with a small real SQLite + gzip/tar fixture and asserts total time below 900 seconds. That proves the
functional and timing contract, not production capacity. At least monthly, rehearse on Linux with representative data,
storage, and the candidate image, recording:

1. UTC start/end for both verifies, SQLite restore, raw restore, and read-only smoke;
2. each CLI `elapsed_seconds`, total SQLite + raw restore wall clock, database/archive bytes, and artifact file count;
3. host, filesystem, storage type, candidate version, and verified digests;
4. whether completion stayed below 900 seconds and the owner of any capacity, storage, or process remediation.

A representative drill over 900 seconds, using mismatched bundles, omitting read-only smoke, or running only on
Windows is not production RTO evidence. Perform power-loss durability and atomic directory cutover drills in a
Linux production-equivalent environment.

## Exit Codes And Failure Handling

- `0`: create, verify, or isolated restore succeeded; stdout is JSON containing `elapsed_seconds`.
- `1`: input, hash, gzip, tar, SQLite reference, path, or safety contract failure.
- `2`: unclassified operating-system failure; the CLI suppresses raw absolute-path exceptions.

Do not delete fields, recalculate a manifest, or use an extraction tool that ignores unsafe members. Preserve the
original bundle and stderr, fix the source, or create a new complete bound bundle. If restore reports incomplete
cleanup, keep services stopped and inspect `.target-name.restore-*` plus any partial target before proceeding.
