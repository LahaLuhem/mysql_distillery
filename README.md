# mysql_distillery

Distill a running MySQL **server** (one host, one or more databases) into
per-component snapshots — schema, data, constraints, views, routines, triggers,
events, and a checksummed manifest — so every database on that server can be
faithfully reconstructed elsewhere (e.g. a preloaded test image, an ephemeral
fixture DB, or a CI-side restore step). Multiple databases distilled in a
single run land in sibling subdirectories under the same output root
(`./snapshots/<database>/…`).

Data is streamed through [DuckDB]'s MySQL extension, so multi-GB tables don't
blow up memory and the export can run in parallel table-by-table.

[DuckDB]: https://duckdb.org/docs/extensions/mysql

---

## Features

- **Component-wise distillation.** Each concern lives in its own module and writes
  its own subdirectory, so artifacts can be diffed and reviewed independently.

  | Component          | Output                                           | Notes |
  |--------------------|--------------------------------------------------|-------|
  | `schema`           | `schema/<table>.sql`                             | `CREATE TABLE` with foreign keys stripped |
  | `constraints`      | `constraints/<db>.sql`                           | foreign_keys as `ALTER TABLE … ADD CONSTRAINT …`, applied after data load |
  | `data`             | `data/<table>.parquet`                           | ZSTD-compressed Parquet, one file per table, parallel |
  | `views`            | `views/<view>.sql`                               | `DEFINER=` stripped so restores aren't tied to a specific user |
  | `routines`         | `routines/<name>.sql`                            | Procedures + functions, `DELIMITER $$` wrapped, `DEFINER=` stripped |
  | `triggers`         | `triggers/<name>.sql`                            | `DELIMITER $$` wrapped, `DEFINER=` stripped |
  | `events`           | `events/<name>.sql`                              | `DELIMITER $$` wrapped, `DEFINER=` stripped |
  | `metadata`         | `metadata/auto_increment.yaml` + `manifest.yaml` | Source info, row counts, SHA256 of every other artifact |
  | `data_quality` ¹   | `logs/data_quality.yaml`                         | **Default OFF** diagnostic scan — gate with `--data-quality-report` / `--data-quality-only` (see "Data-quality report" below) |

  ¹ Not part of the default selection. `_DEFAULT_OFF` in `extract.py` keeps it out of every normal extract run because the scan is read-heavy (COUNT per date col, per NOT-NULL non-date col, per TEXT/BLOB col, plus foreign_key-orphan LEFT JOINs).

- **Parallel by default.** Within a database, components run concurrently in a
  thread pool; the `data` component also parallelizes per-table exports.
  `metadata` always runs last for each database because it checksums the other
  components' output.

- **Multi-database in one run.** Pass `--db` more than once (or as a comma-
  separated list, or via `MYSQL_DATABASES=foo,bar`) and each database is
  distilled into its own subdirectory under `--out`, using the same MySQL
  connection credentials. Databases are processed sequentially to keep log
  output readable and memory predictable; parallelism stays inside a single DB.

- **Standalone component CLIs.** Every component can be run on its own:
  `python -m mysql_distillery.components.schema --out ./snapshots …`. Useful for
  re-running a single piece after a failure.

- **Safe by default.** Connecting to anything other than `localhost`/`127.0.0.1`
  requires `--prod`, which prints a red warning and asks for typed confirmation.

- **Credentials from the environment.** All flags fall back to `MYSQL_*` env vars
  (see `.env.example`). Passwords are redacted in logs and never written to the
  manifest.

- **DDL cleanup built-in.** foreign_keys are extracted separately so load order doesn't
  matter. `DEFINER=` clauses are stripped from views/routines/triggers/events
  so a restore doesn't need the same MySQL users as the source.

- **Type warnings for round-trip-risky columns.** `json`, `bit`, `geometry`,
  `enum`, `set`, `binary`, `varbinary`, `blob` columns are flagged on the
  extraction summary so a reviewer knows to double-check them on restore.

- **Unit-tested regex helpers.** `strip_foreign_keys` and `strip_definer` have
  their own test suites (see `tests/`) — no MySQL instance required.

---

## Project layout

```
mysql_distillery/
├── pyproject.toml
├── .env.example
├── README.md                      ← you are here
├── src/
│   ├── main.py                        ← `python src/main.py …` → orchestrator
│   └── mysql_distillery/
│       ├── extract.py                 ← orchestrator CLI
│       ├── components/                ← one module per extractable concern
│       │   ├── schema.py
│       │   ├── constraints.py
│       │   ├── data.py
│       │   ├── nullable_zerodates.py    ← per-table sidecars for NULLABLE zero-date rehydration
│       │   ├── data_quality.py          ← opt-in diagnostic scan (default OFF)
│       │   ├── views.py
│       │   ├── routines.py
│       │   ├── triggers.py
│       │   ├── events.py
│       │   └── metadata.py
│       └── data/                      ← shared types, models, enums, utils
│           ├── models/                ←   domain objects with behavior
│           │   └── server_connection_config.py
│           ├── dtos/                  ←   plain data carriers
│           │   └── component_result.py
│           ├── enums/                 ←   enumerated types / literals
│           │   └── component_status.py
│           └── utils/                 ←   stateless helpers, one module per concern
│               ├── logging.py         ←     per-component file + stdout logger
│               ├── duckdb.py          ←     DuckDB connection factory
│               ├── safety.py          ←     --prod guardrail
│               ├── ddl.py             ←     regex DDL cleanup (foreign_keys, DEFINER)
│               ├── files.py           ←     sha256, write_text, empty-dir check
│               └── cli.py             ←     shared click scaffolding
└── tests/
    ├── test_common.py                     ← ServerConnectionConfig + sha256_file
    ├── test_schema_foreign_key_strip.py   ← strip_foreign_keys on realistic DDL
    ├── test_ddl_definer_strip.py          ← strip_definer on view/routine/trigger/event DDL
    └── test_data_quality.py               ← data_quality YAML/helpers (inf-ratio, totals)
```

---

## Installation

Requires Python 3.11+ and [uv] (or plain `pip`).

```bash
uv sync
```

That installs `duckdb`, `pymysql`, `pyyaml`, `click`, `rich`, and the dev-only
`pytest` / `ruff`.

[uv]: https://docs.astral.sh/uv/

---

## Configuration

Copy `.env.example` to `.env` and fill it in:

```bash
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=your_password
# Single DB or comma-separated list for multi-DB — each lands in its own subdir.
MYSQL_DATABASES=mydb,mydb_audit
OUTPUT_DIR=./snapshots
```

Every field can also be passed on the command line (`--host`, `--port`, …); CLI
flags override env vars. Nothing is hard-coded — if a required field is missing
at connect time you get a `RuntimeError` pointing back to `.env.example`.

`.env` is auto-loaded via [python-dotenv] at the top of every entry point
(`mysql_distillery`, `python src/main.py`, each `python -m mysql_distillery.components.*`),
so you don't need `set -a && source .env` or an external env loader. Real
environment variables always take precedence over values in `.env`.

[python-dotenv]: https://pypi.org/project/python-dotenv/

---

## Usage

### Distill a single database

```bash
uv run python src/main.py \
  --host localhost --port 3306 \
  --user root --password your_password \
  --db mydb \
  --out ./snapshots
```

Output lands in `./snapshots/mydb/`.

Or — since `mysql_distillery` is registered as a script in `pyproject.toml`:

```bash
uv run mysql_distillery --out ./snapshots
```

(with credentials, including `MYSQL_DATABASES`, pulled from `.env` / environment).

### Distill multiple databases in one run

Pass `--db` more than once:

```bash
uv run mysql_distillery \
  --db mydb \
  --db mydb_audit \
  --out ./snapshots
```

…or as a single comma-separated value:

```bash
uv run mysql_distillery --db mydb,mydb_audit --out ./snapshots
```

…or via the env var (`MYSQL_DATABASES=mydb,mydb_audit`) with
no `--db` flag at all. Artifacts land in:

```
./snapshots/
├── mydb/      ← components for DB #1
└── mydb_audit/  ← components for DB #2
```

Each DB is distilled sequentially, with per-component parallelism inside it.
The end-of-run summary table has a `Database` column so you can tell per-DB
failures apart.

### Flags

| Flag              | Default | Purpose |
|-------------------|---------|---------|
| `--host`          | `$MYSQL_HOST` | MySQL host |
| `--port`          | `$MYSQL_PORT` / 3306 | MySQL port |
| `--user`          | `$MYSQL_USER` | MySQL user |
| `--password`      | `$MYSQL_PASSWORD` | MySQL password |
| `--db`            | `$MYSQL_DATABASES` | Database to extract. Repeatable (`--db a --db b`) or comma-separated. Env var is also comma-separatable. |
| `--out`           | `$OUTPUT_DIR` / `./snapshots` | Parent output directory. Each DB lands in `<out>/<database>/`. |
| `--only`          | all | Comma-separated list of components to run |
| `--skip`          | none | Comma-separated list of components to skip |
| `--workers`       | 8 | Top-level parallelism across components (per DB) |
| `--data-workers`  | 4 | Per-table parallelism inside the `data` component |
| `--prod`          | off | **Required** for non-local hosts; prompts to confirm |
| `--force`         | off | Overwrite non-empty per-DB output subdirectories |
| `--data-quality-report` | off | Add the `data_quality` diagnostic scan to this run (it's otherwise default-off — see "Data-quality report" below). Read-only. Adds minutes on large DBs. |
| `--data-quality-only`   | off | Run ONLY the data-quality scan; skip every other component. Implies `--data-quality-report`. Useful for triage against an already-extracted DB — won't overwrite existing dumps (the empty-dir preflight is skipped). |

### Run a single component

Every component module is its own `__main__` and uses the same multi-DB
interface as the orchestrator:

```bash
# Just re-emit the data for one DB
uv run python -m mysql_distillery.components.data \
  --db mydb --out ./snapshots --max-workers 8

# Re-run metadata for both DBs after editing other artifacts by hand
uv run python -m mysql_distillery.components.metadata \
  --db mydb --db mydb_audit --out ./snapshots
```

### Distill only certain components

```bash
uv run mysql_distillery --out ./snapshots --only schema,constraints,data,metadata
uv run mysql_distillery --out ./snapshots --skip events,triggers
```

`metadata` always runs last when included; it reads the other components'
output to produce checksums and row counts.

### Distill from production (use with care)

Non-local hosts are rejected unless `--prod` is passed. `--prod` then prints a
red warning and refuses to continue unless you type `yes`:

```bash
uv run mysql_distillery --host prod-db.internal --user readonly \
  --db mydb --db mydb_audit \
  --out ./snapshots --prod
```

The run is read-only (the DuckDB `ATTACH` is `READ_ONLY`), but you are still
opening a connection to production — don't paste this into CI without review.

### Data-quality report

Opt-in diagnostic scan that surfaces source-DB integrity issues the
extract/restore pipeline silently tolerates (zero-dates, NOT NULL
violations, orphaned foreign_keys, prim_key-less tables, fat BLOB rows, charset drift).
Off by default — the scan is read-only but adds minutes on large schemas
(a ~480-table source takes ~4 min at `--data-workers 4`).

Two invocation modes:

```bash
# Full extract PLUS the scan (runs alongside the normal components in the pool).
uv run mysql_distillery --db mydb --out ./snapshots --data-quality-report

# Scan only — skip every other component. Doesn't touch existing per-DB dumps
# (the empty-dir preflight is skipped). Useful for triage on an already-extracted
# DB, or as a pre-flight against a suspicious source.
uv run mysql_distillery --db mydb --out ./snapshots --data-quality-only
```

Outputs `<out>/<db>/logs/data_quality.yaml` (machine-readable, diffable;
shipped **out** of distributed images via `.dockerignore`'s `**/logs/`
rule) plus per-category rich tables to stderr. Scan completion is always
`status=ok` — the non-zero-count signal lives in the `Notes / error`
column of the extraction summary.

Full per-check rationale (why prim_key zero-dates are a separate class from
non-prim_key, why date cols are excluded from NULL checks, composite-foreign_key
handling, outlier thresholds, etc.): [APPENDIX.md#data-quality-scan](./APPENDIX.md#data-quality-scan).

---

## Output layout

A completed run with two databases looks like:

```
./snapshots/
├── mydb/
│   ├── schema/              one .sql per base table (foreign_keys stripped)
│   ├── constraints/         one .sql per database (ALTER TABLE … ADD CONSTRAINT …)
│   ├── data/                one .parquet per base table (ZSTD)
│   ├── views/               one .sql per view
│   ├── routines/            one .sql per procedure/function
│   ├── triggers/            one .sql per trigger
│   ├── events/              one .sql per event
│   ├── metadata/
│   │   └── auto_increment.yaml
│   ├── logs/                per-component <name>.log + data_quality.yaml (when --data-quality-report)
│   └── manifest.yaml        source info + row counts + per-file SHA256s
└── mydb_audit/
    └── …                    ← same layout, independent of the sibling DB
```

Each per-DB subdirectory is self-contained — the manifest, checksums, and logs
only describe that database. Re-running extraction for one DB does not touch
the sibling subdirs. The pre-flight empty-check (and `--force`) is scoped to
the per-DB subdir, not the parent `--out` directory.

Applying a single DB's snapshot to an empty MySQL (the job of the
preloaded-image builder elsewhere in `mavis-anon-base`) goes roughly:

1. `<db>/schema/*.sql` — create tables, no foreign_keys
2. `<db>/data/*.parquet` — load rows (order doesn't matter without foreign_keys)
3. `<db>/constraints/<db>.sql` — add foreign_keys
4. `<db>/views/*.sql`, `<db>/routines/*.sql`, `<db>/triggers/*.sql`, `<db>/events/*.sql`
5. Apply `<db>/metadata/auto_increment.yaml` so new inserts don't collide with existing ids

---

## Testing

```bash
uv run pytest -q
```

Tests are fully offline — they only exercise the regex helpers and
`ServerConnectionConfig`, no MySQL instance needed. When adding a new DDL
quirk, extend `tests/test_schema_foreign_key_strip.py` or `tests/test_ddl_definer_strip.py`.

---

## Security notes

- **The run is read-only** but still opens a live MySQL connection; prefer a
  dedicated read-only user.
- **Passwords are redacted** in `ServerConnectionConfig.to_safe_dict()` and
  never written to `manifest.yaml`.
- **`--prod` is a guardrail, not an authorization model** — don't rely on it in
  automation; instead, don't give your automation credentials to production at all.
- **Anonymization is out of scope for this tool.** Raw distillery output may still
  contain PII; the *preloaded image builder* downstream is responsible for
  anonymizing sensitive columns before publishing. Don't share untouched output.
