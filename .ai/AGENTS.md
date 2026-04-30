# AGENTS.md — `mysql_distillery`

Tool-agnostic brief for any coding agent (Copilot, Cursor, Codex, Claude Code,
…) working inside `mysql_distillery/`. Claude-Code-specific guidance lives in
[CLAUDE.md](./CLAUDE.md).

## Project goal
Distill a running MySQL **server** (one host, one or more databases) into
**per-component snapshot artifacts** (schema, constraints, data, views,
routines, triggers, events, metadata) so every database on that server can be
faithfully reconstructed elsewhere — typically into a preloaded MySQL Docker
image for team testing. Data streams through DuckDB's MySQL extension (no
Pandas); DDL that DuckDB can't prepare goes through `pymysql` directly.

## Stack
- **Python ≥ 3.11**, managed with **`uv`** (never `pip`/`venv` directly).
- **DuckDB** (mysql extension) for streaming data + `information_schema` reads.
- **`pymysql`** for `SHOW CREATE …` (DuckDB can't prepare those — see pitfalls).
- **`click`** CLIs, **`rich`** console output, **`pyyaml`** for the manifest, **`python-dotenv`** for `.env`.
- **Dev**: `pytest`, `ruff` (lint + format).

## File map
```
src/mysql_distillery/
├── extract.py                     # mysql_distillery orchestrator (click CLI)
├── components/                    # one module per snapshot concern
│   ├── schema.py  constraints.py  data.py   views.py
│   ├── routines.py  triggers.py   events.py  metadata.py
└── data/
    ├── dtos/component_result.py   # ComponentResult (status/duration/artifacts/notes)
    ├── enums/component_status.py
    ├── models/server_connection_config.py  # frozen dataclass, env-driven
    └── utils/
        ├── duckdb.py   mysql.py   cli.py
        ├── ddl.py      files.py   logging.py   safety.py
tests/                             # pytest; run with `uv run pytest`
```
Output layout: `<out>/<database>/{schema,constraints,data,views,routines,triggers,events,metadata}/…` plus `manifest.yaml`.

## Hard rules
1. **Never hardcode credentials.** Always go through `ServerConnectionConfig.from_env` / `from_overrides`. Log via `cfg.to_safe_dict()` — passwords never hit logs or the manifest.
2. **`--prod` guardrail is sacred.** Any non-local host refuses to connect without `--prod` + typed "yes" confirmation (`data/utils/safety.py`). Never bypass, never weaken.
3. **DuckDB can't prepare `SHOW CREATE …`** → use `get_pymysql_connection(cfg)` + `show_create(...)` from `data/utils/mysql.py`. `information_schema` SELECTs stay on DuckDB.
4. **Parquet is the row-count source of truth** for the manifest. `metadata` re-reads counts from the written files, not from MySQL — don't "optimize" this away.
5. **foreign_keys are stripped** from `CREATE TABLE` in `schema` and re-emitted by `constraints` as separate `ALTER TABLE`s so load order is irrelevant. Don't re-introduce foreign_keys into schema files.
6. **`DEFINER=` must be stripped** from views/routines/triggers/events so restores don't depend on source MySQL users. Use `strip_definer()`.
7. **DuckDB connections are not thread-safe.** Every worker gets its own via `get_duckdb_connection(cfg)`. Same applies to `pymysql`.
8. **Before claiming done:** `uv run pytest -q` **and** `uv run ruff check` (add `ruff format --check` if formatting). No exceptions.

## Style (Python-as-strongly-typed)
The user prefers Dart-flavoured typing: **everything annotated, nothing vague**.
- **Annotate every function signature and every module-level constant.** No bare `def foo(x)`.
- **Use `Final`** for module-level constants and any value that shouldn't be reassigned: `_LOCAL_HOSTS: Final[frozenset[str]] = frozenset({...})`.
- **Make nullability explicit** via `T | None` (preferred, modern) or `Optional[T]`. Never rely on "missing = None" implicitly.
- **`from __future__ import annotations`** at the top of every module (already the house style).
- **Prefer `@dataclass(frozen=True)`** for value objects; models shouldn't be mutable by default.
- **Return concrete types**, not `Any`. If you reach for `Any`, stop and reconsider.
- Don't write Java. **No getters/setters, no interface-per-class, no "Abstract…Factory".** Use protocols/dataclasses/typed dicts when they add clarity, not ceremony.
- Docstrings: short, "why" over "what". The type system should carry the "what".
- **Abbreviations, not initialisms, for domain terms.** Use `auto_inc` / `prim_key` / `foreign_key` everywhere — identifiers, log messages, docstrings, comments. Never `AI` / `PK` / `FK`. Applies to class names (`_MissingPrimKey`, not `_MissingPk`), local vars, column labels, and any user-facing string.

## How to add a new component
1. Create `src/mysql_distillery/components/<name>.py` with:
   - A `run(cfg: ServerConnectionConfig, out_dir: Path, *, max_workers: int = 1) -> ComponentResult` that wraps the body in `with ComponentResult.timed("<name>") as result:`.
   - A `setup_logger("<name>", out_dir)` for per-component logs.
   - Writes under `out_dir / "<name>" / …` and appends every file to `result.artifacts`.
   - `if __name__ == "__main__": make_component_cli("<name>", run)()` so it runs standalone.
2. Register it in `extract.py`:
   - If it can run parallel with peers → add to `_PARALLEL_COMPONENTS`.
   - If it needs other components' output (like `metadata`) → make it `_ALWAYS_LAST`.
3. Add it to `_COMPONENT_DIRS` in `components/metadata.py` so its artifacts get checksummed into the manifest.
4. Mirror the README's component table.
5. Tests under `tests/` (parse/cleanup logic as pure unit tests — don't require a live MySQL).

## Known pitfalls
- **`Failed to fetch return types for query 'SHOW CREATE …'`** — DuckDB's `mysql_query()` prepares statements; MySQL can't prepare DDL-reflection queries. Fix: use `show_create()` from `data/utils/mysql.py`.
- **`SHOW CREATE TABLE` returning bytes** on some MySQL configurations — `show_create()` already decodes defensively; keep that.
- **`COPY … TO '…'`** interpolates identifiers (parameter binding isn't allowed in the literal). Only splice names sourced from `information_schema`, never user input.
- **`ONLY_FULL_GROUP_BY`** can make legacy views refuse to restore. The restoring MySQL needs a relaxed `sql_mode`; the manifest captures the source `sql_mode` for reference.
- **Blob/json/bit/geometry/enum/set/varbinary round-trips** through Parquet need review on restore. `data.py` already surfaces them as `notes` on `ComponentResult`; see [`APPENDIX.md#type-warnings`](./APPENDIX.md#type-warnings) for the rationale. Don't silence those warnings.
- **`metadata` must run after everything else** (it checksums their artifacts). Orchestrator enforces this; don't reorder.
- **Zero-date handling, BLOB restore path, `cryptography` dep, preflight, data-quality scan** → full rationale in [`APPENDIX.md`](./APPENDIX.md). Read before altering.

## Documentation convention
- **APPENDIX.md is the source of truth for rationale.** Pitfalls / hard rules / workflows stay here; the "why we do it this way" essays live in `APPENDIX.md`.
- **Explicit `<a id="…">` anchors** sit above every APPENDIX heading. Link to sections via the anchor, not the heading text.
- **Anchor stability is load-bearing.** When renaming a heading, keep the existing anchor. If you must change it, `rg '#<old-anchor>'` across the repo and update every caller in the same change.

## Env vars
`MYSQL_HOST`, `MYSQL_PORT` (default `3306`), `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASES` (single name or comma-separated list — first wins when only one is needed). Kept in `.env` (gitignored); `.env.example` documents the shape.
