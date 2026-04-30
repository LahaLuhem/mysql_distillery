"""Tests for ServerConnectionConfig and the sha256 file helper."""
from __future__ import annotations

import pytest

from mysql_distillery.data.models import ServerConnectionConfig
from mysql_distillery.data.utils.files import sha256_file


def test_from_env_reads_all_vars(monkeypatch):
    monkeypatch.setenv("MYSQL_HOST", "h")
    monkeypatch.setenv("MYSQL_PORT", "3307")
    monkeypatch.setenv("MYSQL_USER", "u")
    monkeypatch.setenv("MYSQL_PASSWORD", "p")
    monkeypatch.setenv("MYSQL_DATABASES", "db")

    cfg = ServerConnectionConfig.from_env()
    assert cfg == ServerConnectionConfig(host="h", port=3307, user="u", password="p", database="db")


def test_from_env_missing_var_raises(monkeypatch):
    for k in ("MYSQL_HOST", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASES"):
        monkeypatch.delenv(k, raising=False)
    with pytest.raises(RuntimeError, match="Missing required env var"):
        ServerConnectionConfig.from_env()


def test_from_overrides_overlays_on_env(monkeypatch):
    monkeypatch.setenv("MYSQL_HOST", "env-host")
    monkeypatch.setenv("MYSQL_USER", "env-user")
    monkeypatch.setenv("MYSQL_PASSWORD", "env-pass")
    monkeypatch.setenv("MYSQL_DATABASES", "env-db")

    cfg = ServerConnectionConfig.from_overrides(host="cli-host", database="cli-db")
    assert cfg.host == "cli-host"
    assert cfg.user == "env-user"  # not overridden
    assert cfg.database == "cli-db"


def test_to_safe_dict_redacts_password():
    cfg = ServerConnectionConfig(host="h", port=1, user="u", password="secret", database="d")
    safe = cfg.to_safe_dict()
    assert safe["password"] == "***REDACTED***"
    # Password doesn't leak via string form either.
    assert "secret" not in str(safe)


def test_duckdb_attach_sql_shape():
    cfg = ServerConnectionConfig(host="h", port=3306, user="u", password="p", database="d")
    sql = cfg.duckdb_attach_sql("mydb")
    assert "ATTACH 'host=h" in sql
    assert "database=d" in sql
    assert "AS mydb" in sql
    assert "TYPE mysql" in sql
    assert "READ_ONLY" in sql


def test_validate_raises_when_incomplete():
    cfg = ServerConnectionConfig(host="", port=3306, user="u", password="p", database="d")
    with pytest.raises(RuntimeError, match="missing: host"):
        cfg.validate()


def test_sha256_file_matches_known_value(tmp_path):
    p = tmp_path / "x.txt"
    p.write_bytes(b"hello")
    # echo -n "hello" | shasum -a 256
    assert (
        sha256_file(p)
        == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    )
