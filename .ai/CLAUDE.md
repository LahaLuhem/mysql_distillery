# CLAUDE.md — `mysql_distillery`

Claude-Code-specific guidance. For project facts / hard rules / style / pitfalls,
read [AGENTS.md](./AGENTS.md) first — this file only adds the Claude-Code layer.

## Tone & communication
- **Terse.** The user reads diffs — don't recap what you just did.
- **Explain the *why*, not the *what*.** Type annotations, the diff, and the file:line links carry the "what".
- **When you're uncertain about an invariant, say so and ask.** Don't invent behavior.
- **Use `file.py:42`-style links** when referencing code.

## Tool preferences
- **Read / Edit / Grep / Glob** over `cat` / `sed` / `grep` / `find`. Always.
- **Bash** only for things with no dedicated tool (running `uv`, `pytest`, `ruff`, `git`, the CLI under test).
- **Agent tool** for open-ended multi-step searches or when you need to protect the main context from large tool outputs. Don't delegate trivial lookups.

## Auto-memory conventions for this project
- **`project` memories** — save scope/constraints the user states aloud (e.g. "we're freezing new components until X", "this DB is EOL in Q3"). Convert relative dates to absolute.
- **`feedback` memories** — save any correction *or* any non-obvious approach the user explicitly ratified. Always include **Why** and **How to apply** lines. Typing style (Final, `T | None`, no Java ceremony) is a standing feedback memory worth keeping.
- **`reference` memories** — external dashboards, Linear/Jira projects, Slack channels that relate to `mavis-anon`. Not the code paths — those live in AGENTS.md.
- **Do NOT save**: component file paths, architecture summaries, CLI flags, env var names. All derivable from the repo. Re-deriving is safer than a stale memory.
- **Before acting on a memory**, verify the named file/flag still exists (grep or read). A memory is a claim-at-write-time, not a fact-now.

## Plan mode / when to stop and think
Enter plan mode (or propose a plan in-chat before editing) when:
- A change touches more than **3 components** at once, or the orchestrator + any component.
- You're adding/removing a component, or changing `ComponentResult` / `ServerConnectionConfig`.
- The change affects on-disk output layout or the manifest schema (downstream restorers depend on it).
- You're about to touch `safety.py`, credential handling, or anything that could widen the `--prod` blast radius.

For single-file, single-concern fixes (like the DuckDB→pymysql swap): just do it.

## Commit / PR etiquette
- **Never commit without being asked.** Not after a fix, not "to checkpoint", not ever. Show the diff and wait.
- **Never push without being asked.** Especially not to `main`.
- **Never `--amend`** unless the user asked — create a new commit instead.
- **Never `--no-verify`**, **never `git add -A`** (use named paths).
- Commit messages: match the repo's existing style (short imperative subject, no Claude-authored footer unless asked). Recent examples: `Support multiple DBs directly`, `Highly segregated data extraction`.
- When asked for a commit: show `git status` + `git diff` first, draft the message, wait for approval before running `git commit`.

## Forbidden / confirm-first actions
- **Never** `rm -rf snapshots/` (or whatever `--out` points to) — that's distilled data, often large and re-running is slow.
- **Never** drop/overwrite `.env` or `.env.example`. Ask if credentials seem wrong.
- **Never** run `mysql_distillery` / the orchestrator against anything but `localhost`/`127.0.0.1` without explicit user approval for that exact host.
- **Never** modify `safety.py`'s `_LOCAL_HOSTS` set, weaken `warn_if_prod`, or add new "bypass" flags.
- **Never** delete files under `.venv/`, `uv.lock`, or `pyproject.toml` without approval.
- **Destructive git** (`reset --hard`, `push --force`, `branch -D`, `clean -fd`) → ask.
- **Long-running commands** (full distillation runs, parallel worker stress tests) → start with `run_in_background` and monitor rather than blocking the session.

## Typing expectations (project-specific)
The user wants Python to feel Dart-ish without Java bloat. When you write or edit code here:
- Annotate every parameter and return. No bare `x`.
- Module-level constants get `Final[...]`: `_TYPES_TO_WARN: Final[frozenset[str]] = frozenset({...})`.
- Nullability is explicit: `cfg: ServerConnectionConfig | None = None`, not implicit "defaults to None means optional".
- `@dataclass(frozen=True)` for value objects (see `ServerConnectionConfig`). Don't mutate.
- `from __future__ import annotations` at top of every module.
- Don't reach for `Any`. If you do, justify it in a comment.

## Definition of done
Before you say "done":
1. `uv run pytest -q` — green.
2. `uv run ruff check src tests` — clean (and `ruff format --check` if you touched formatting).
3. Type annotations in place on everything you added or changed.
4. If you edited a component, confirm its artifacts still end up in `_COMPONENT_DIRS` (metadata) and that the manifest still covers it.
5. State clearly what you did NOT verify (e.g. "didn't run against a live MySQL — only unit tests").
