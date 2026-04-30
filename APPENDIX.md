# APPENDIX — `mysql_distillery`

Consolidated source of truth for distillery-side design decisions and
non-obvious technical trade-offs.

READMEs and `.ai/AGENTS.md` reference sections here by anchor. **Headings
below are load-bearing** — each carries an explicit `<a id="…">` anchor
immediately above it. When renaming a heading, keep the anchor stable or
grep-and-update every caller.

---

<a id="preflight-before-parallel-work"></a>
## Preflight before parallel work

- The distillery forks parallel workers per table. Without a preflight, an auth/host/db failure manifests as N identical worker errors with no clean single message.
- **Decision:** `preflight_check()` runs a single `SELECT 1` via PyMySQL at the top of `data.run()` and raises a human-readable `RuntimeError` on failure. Cheap, and it short-circuits the whole pipeline before any workers start.

---

<a id="data-quality-scan"></a>
## Data-quality scan (`data_quality` component, opt-in)

- **Problem:** legacy MySQL sources (running with permissive `sql_mode`) accumulate data that violates strict constraints — zero-dates on NOT NULL cols, NULLs sneaked into NOT NULL cols, orphaned foreign_key refs, tables without a PRIMARY KEY, pathologically large BLOBs, charset drift. None of it blocks an extract or a restore, but every cycle we "rediscover" it — each case surfaces as either a compare mismatch or a restore-side workaround (see the `srkrt.MUTADT` unrestorable-prim_key-zero-date bug at 2026-04-21). A standing diagnostic makes it loud, documented, and greppable.
- **Decision:** `src/mysql_distillery/components/data_quality.py` — 8-check read-only scan, a proper `ComponentResult`-returning component that slots into the orchestrator's parallel pool. Registered in `_PARALLEL_COMPONENTS` but excluded from the default selection via `_DEFAULT_OFF` in `extract.py`. Gated by `--data-quality-report` (adds the scan to a normal run) or `--data-quality-only` (skips every other component — for triage). Reuses `--data-workers` for per-scan parallelism (two knobs felt redundant for the same read-heavy-per-table cost profile).
- **Checks** — always all 8 when enabled:
  1. Tables without `PRIMARY KEY`.
  2. Zero-dates in **NOT NULL prim_key cols** — *unrestorable-class* under the standard relax/fill/restore flow, because InnoDB forbids `NULL` on prim_key cols; downstream restore tooling needs a hex-TSV `COALESCE` path to preserve them.
  3. Zero-dates in **NOT NULL non-prim_key cols** — *relax/fill/restore-class* (the normal flow).
  4. Zero-dates in **NULLABLE** date cols — *sidecar-class* (handled by the `nullable_zerodates` component, rehydrated at restore step 3. c)).
  5. NULLs in **NOT NULL non-date cols** — date types are excluded because MySQL's `WHERE col IS NULL` matches zero-dates on date cols regardless of `sql_mode` (verified empirically 2026-04-21 against MySQL 8.0 with `sql_mode=''`), which would duplicate checks 2-4 entirely. Non-date NOT-NULL NULLs are genuine data corruption.
  6. Orphaned foreign_key refs — single-column foreign_keys only; composite foreign_keys are logged and skipped because their orphan semantics need a multi-column join that's ambiguous without a composite uniqueness guarantee on the parent.
  7. Row-length outliers on `TEXT`/`BLOB` cols — `MAX(OCTET_LENGTH) ≥ 1 MiB` AND `≥ 10 × AVG`. `VARCHAR` deliberately excluded (its upper bound is schema-declared; "outlier" there is usually noise for hundreds of narrow cols).
  8. Charset mismatches — table vs DB default; column vs table.
- **Output:** `<db>/logs/data_quality.yaml` (machine-readable, diffable) plus a rich table per non-empty category to stderr (empty categories suppressed to keep the scream scannable). **The yaml lives under `logs/` specifically so `.dockerignore`'s `**/logs/` rule keeps it out of distributed images** — none of it is consumed at restore time.
- **Cost:** ~3-4 min on a ~480-table DB with `--data-workers 4`. Scans are per-column `COUNT(*)` on every date / NOT-NULL non-date / TEXT/BLOB col, plus one LEFT-JOIN per foreign_key constraint. Not cheap, but far cheaper than rediscovering a data problem as a restore bug a week later.
- **Why not always on:** diagnostic checks shouldn't tax every routine cycle.

---

<a id="type-warnings"></a>
## Type warnings for round-trip-risky columns

- **Surfaced by** the `data` component as `ComponentResult.notes` on the extraction summary.
- **Flagged types:** `json`, `bit`, `geometry`, `enum`, `set`, `binary`, `varbinary`, `blob`. These either round-trip lossily through Parquet or need restore-side handling a reviewer should eyeball. The list is tuned to surface cases a reviewer should eyeball on restore; don't silence the warnings.
- **Not an error.** The component still succeeds. The warnings steer review attention, not pipeline failure.
