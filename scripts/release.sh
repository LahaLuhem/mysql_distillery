#!/usr/bin/env bash
# ===========================================================================
# release.sh
#
# Cut a versioned PyPI release for mysql_distillery.
#
# Hybrid model: this script bumps the version in pyproject.toml, sanity-builds,
# commits the bump, tags vX.Y.Z, and pushes. The GitHub Action at
# .github/workflows/publish.yml then publishes to PyPI via OIDC trusted
# publishing — no API token on this machine.
#
# `--testpypi` mode skips the commit/tag/push and instead publishes the
# locally-built artifacts to TestPyPI from this machine, using
# $UV_PUBLISH_TOKEN. Useful as a smoke test before cutting a real release.
#
# Safe by default. Preflight refuses to proceed on a dirty tree, the wrong
# branch, an origin mismatch, failing tests/lint, or a tag collision.
#
# Usage:
#   scripts/release.sh                         # fully interactive
#   scripts/release.sh patch                   # bump type set, confirm on TTY
#   scripts/release.sh patch --yes             # non-interactive (CI-style)
#   scripts/release.sh --dry-run               # print plan, no side effects
#   scripts/release.sh patch --testpypi        # bump + build + publish to TestPyPI
#                                              # (no commit, no tag, no push)
#
# Sequence (real release path):
#   1. uv version --bump <type>                 # writes pyproject.toml
#   2. uv build --no-sources                    # ./dist/*.whl + *.tar.gz
#   3. git commit -am "Release vX.Y.Z"
#   4. git tag vX.Y.Z
#   5. git push origin main
#   6. git push origin vX.Y.Z                   # triggers publish.yml Action
# ===========================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

MAIN_BRANCH="main"
PYPI_PROJECT_NAME="mysql_distillery"
GITHUB_OWNER="LahaLuhem"

BUMP=""
YES=0
DRY_RUN=0
TESTPYPI=0

usage() {
    cat <<'USAGE'
release.sh — cut a versioned PyPI release.

Hybrid: this script bumps + tags + pushes. The .github/workflows/publish.yml
Action publishes the resulting tagged commit to PyPI via OIDC trusted publishing.

Usage:
  scripts/release.sh [BUMP] [OPTIONS]

Arguments:
  BUMP            one of: major, minor, patch  (prompted if omitted on a TTY)

Options:
  -y, --yes       skip the confirmation prompt (required for non-TTY)
  -n, --dry-run   print the plan and exit without side effects
  --testpypi      publish to TestPyPI from this machine instead of cutting a
                  real release; needs $UV_PUBLISH_TOKEN. Bumps + builds, but
                  does NOT commit / tag / push.
  -h, --help      show this message

Preflight (always; --testpypi included):
  - working tree clean
  - on `main`
  - in sync with origin/main (fetches first)
  - `uv run pytest -q` passes
  - `uv run ruff check src tests` clean

Real release path also requires:
  - tag vX.Y.Z is unused locally and on origin

--testpypi path also requires:
  - $UV_PUBLISH_TOKEN is set (a TestPyPI API token)
USAGE
}

while (($#)); do
    case "$1" in
        major|minor|patch) BUMP="$1" ;;
        -y|--yes)          YES=1 ;;
        -n|--dry-run)      DRY_RUN=1 ;;
        --testpypi)        TESTPYPI=1 ;;
        -h|--help)         usage; exit 0 ;;
        *) printf 'unknown arg: %s (use --help)\n' "$1" >&2; exit 2 ;;
    esac
    shift
done

log()  { printf '[release] %s\n' "$*"; }
step() { printf '\n[release] == %s ==\n' "$*"; }
err()  { printf '[release] ERROR: %s\n' "$*" >&2; }

is_tty() { [ -t 0 ]; }

prompt_bump() {
    local reply
    while :; do
        printf 'Bump type [major/minor/patch] (default: patch): ' >&2
        read -r reply
        reply="${reply:-patch}"
        case "$reply" in
            major|minor|patch) echo "$reply"; return 0 ;;
            *) printf 'Please enter major, minor, or patch.\n' >&2 ;;
        esac
    done
}

# ---------------------------------------------------------------------------
# Resolve BUMP
# ---------------------------------------------------------------------------
if [ -z "$BUMP" ]; then
    if is_tty; then
        BUMP="$(prompt_bump)"
    else
        err 'BUMP argument required in non-interactive mode (one of: major, minor, patch).'
        exit 2
    fi
fi

# ---------------------------------------------------------------------------
# Compute current + new version (uv writes the file when --dry-run is omitted,
# so we use --dry-run here just to peek; the real bump happens later).
# ---------------------------------------------------------------------------
log 'Fetching origin…'
git fetch origin --quiet --tags

current_version="$(uv version --short)"
new_version="$(uv version --bump "$BUMP" --short --dry-run)"
new_tag="v${new_version}"

log "Current version: ${current_version}"
log "New version:     ${new_version}  (${BUMP} bump)  → tag ${new_tag}"

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------
step 'Preflight'
fail=0

# 1) Clean working tree
if [ -n "$(git status --porcelain)" ]; then
    err 'Working tree is dirty. Commit or stash first.'
    fail=1
else
    log 'Working tree clean.'
fi

# 2) On main
current_branch="$(git rev-parse --abbrev-ref HEAD)"
if [ "$current_branch" != "$MAIN_BRANCH" ]; then
    err "Current branch is '$current_branch'; expected '$MAIN_BRANCH'."
    fail=1
else
    log "On branch '$MAIN_BRANCH'."
fi

# 3) In sync with origin/main
local_head="$(git rev-parse HEAD)"
remote_head="$(git rev-parse "origin/${MAIN_BRANCH}" 2>/dev/null || echo '')"
if [ -z "$remote_head" ]; then
    err "origin/${MAIN_BRANCH} not found."
    fail=1
elif [ "$local_head" != "$remote_head" ]; then
    err "HEAD ($local_head) is not at origin/${MAIN_BRANCH} ($remote_head). Pull / push first."
    fail=1
else
    log "In sync with origin/${MAIN_BRANCH}."
fi

# 4) Tests pass
log 'Running pytest…'
if uv run pytest -q >/dev/null 2>&1; then
    log 'pytest green.'
else
    err 'pytest failed. Run `uv run pytest -q` to see details.'
    fail=1
fi

# 5) Ruff clean
log 'Running ruff…'
if uv run ruff check src tests >/dev/null 2>&1; then
    log 'ruff clean.'
else
    err 'ruff found issues. Run `uv run ruff check src tests` to see them.'
    fail=1
fi

# 6) Tag unused (real-release mode only)
if [ "$TESTPYPI" -eq 0 ]; then
    if git rev-parse "refs/tags/${new_tag}" >/dev/null 2>&1; then
        err "Tag '${new_tag}' already exists locally."
        fail=1
    elif git ls-remote --tags origin "refs/tags/${new_tag}" | grep -q .; then
        err "Tag '${new_tag}' already exists on origin."
        fail=1
    else
        log "Tag '${new_tag}' is unused locally and on origin."
    fi
fi

# 7) TestPyPI mode requires a publish token
if [ "$TESTPYPI" -eq 1 ]; then
    if [ -z "${UV_PUBLISH_TOKEN:-}" ]; then
        err '$UV_PUBLISH_TOKEN must be set for --testpypi (a TestPyPI API token).'
        fail=1
    else
        log 'TestPyPI token present in $UV_PUBLISH_TOKEN.'
    fi
fi

if [ "$fail" -eq 1 ]; then
    err 'Preflight failed — aborting.'
    exit 1
fi

# ---------------------------------------------------------------------------
# Plan
# ---------------------------------------------------------------------------
step 'Plan'
if [ "$TESTPYPI" -eq 1 ]; then
    cat <<PLAN
Will execute (TestPyPI), in order:
  1. uv version --bump ${BUMP}        # writes pyproject.toml: ${current_version} → ${new_version}
  2. uv build --no-sources            # ./dist/${PYPI_PROJECT_NAME}-${new_version}{.tar.gz,-py3-none-any.whl}
  3. uv publish --index testpypi      # uploads to https://test.pypi.org

After: pyproject.toml will be modified but NOT committed. Either commit it
manually, or roll back with: git checkout pyproject.toml uv.lock
PLAN
else
    cat <<PLAN
Will execute, in order:
  1. uv version --bump ${BUMP}        # writes pyproject.toml: ${current_version} → ${new_version}
  2. uv build --no-sources            # ./dist/${PYPI_PROJECT_NAME}-${new_version}{.tar.gz,-py3-none-any.whl}
  3. git commit -am "Release ${new_tag}"
  4. git tag ${new_tag}
  5. git push origin ${MAIN_BRANCH}
  6. git push origin ${new_tag}        # GitHub Action then publishes to PyPI
PLAN
fi

if [ "$DRY_RUN" -eq 1 ]; then
    log 'Dry-run mode — nothing executed.'
    exit 0
fi

# ---------------------------------------------------------------------------
# Confirm
# ---------------------------------------------------------------------------
if [ "$YES" -eq 0 ]; then
    if is_tty; then
        printf '\nProceed? [y/N] '
        read -r reply
        case "$reply" in
            y|Y|yes|YES) ;;
            *) log 'Aborted.'; exit 0 ;;
        esac
    else
        err 'Refusing to proceed without --yes in non-interactive mode.'
        exit 2
    fi
fi

# ---------------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------------
step "Bumping version: ${current_version} → ${new_version}"
uv version --bump "$BUMP"

step 'Building wheel + sdist'
rm -rf dist/
uv build --no-sources

if [ "$TESTPYPI" -eq 1 ]; then
    step 'Publishing to TestPyPI'
    uv publish --index testpypi
    log "Uploaded to https://test.pypi.org/project/${PYPI_PROJECT_NAME}/${new_version}/"
    log 'Verify:'
    log "  uv run --index https://test.pypi.org/simple/ --with ${PYPI_PROJECT_NAME}==${new_version} -- ${PYPI_PROJECT_NAME} --help"
    log 'pyproject.toml is modified but NOT committed. Roll back or commit as appropriate.'
    exit 0
fi

step 'Committing version bump'
git commit -am "Release ${new_tag}"

step "Creating git tag ${new_tag}"
git tag "${new_tag}"

step "Pushing ${MAIN_BRANCH} + tag ${new_tag} to origin"
git push origin "${MAIN_BRANCH}"
git push origin "${new_tag}"

step "Released ${new_tag}"
log "Tag pushed → GitHub Action will build and publish to PyPI."
log "Monitor: https://github.com/${GITHUB_OWNER}/${PYPI_PROJECT_NAME}/actions"
log 'Verify (after Action completes):'
log "  uv run --with ${PYPI_PROJECT_NAME}==${new_version} -- ${PYPI_PROJECT_NAME} --help"
