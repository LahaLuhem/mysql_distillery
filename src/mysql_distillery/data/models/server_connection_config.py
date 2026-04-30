"""MySQL connection parameters — loaded from env, overridable from CLI flags.

Frozen so worker threads can share the same instance safely. Never construct
with hard-coded credentials — use :meth:`from_env` or :meth:`from_overrides`.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ServerConnectionConfig:
    """MySQL connection parameters.

    Always construct via :meth:`from_env` or :meth:`from_overrides` —
    never hard-code credentials. Frozen so worker threads can share it safely.
    """

    host: str
    port: int
    user: str
    password: str
    database: str

    @classmethod
    def from_env(cls) -> "ServerConnectionConfig":
        """Read MYSQL_* env vars. Raises if any required var is missing.

        If ``MYSQL_DATABASES`` is a comma-separated list, the first entry is
        used as this config's ``database``. Use :meth:`databases_from_env`
        to get the full list for multi-DB extraction.
        """
        try:
            raw_dbs = os.environ["MYSQL_DATABASES"]
            first_db = raw_dbs.split(",", 1)[0].strip()
            if not first_db:
                raise RuntimeError(
                    "MYSQL_DATABASES is set but empty. "
                    "Provide at least one database name."
                )
            return cls(
                host=os.environ["MYSQL_HOST"],
                port=int(os.environ.get("MYSQL_PORT", "3306")),
                user=os.environ["MYSQL_USER"],
                password=os.environ["MYSQL_PASSWORD"],
                database=first_db,
            )
        except KeyError as exc:
            raise RuntimeError(
                f"Missing required env var: {exc.args[0]}. "
                "See .env.example for the expected variables."
            ) from exc

    @classmethod
    def from_overrides(
        cls,
        *,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
    ) -> "ServerConnectionConfig":
        """Load from env, then override any field that was explicitly passed."""
        base = cls.from_env_or_partial()
        return ServerConnectionConfig(
            host=host or base.host,
            port=port if port is not None else base.port,
            user=user or base.user,
            password=password if password is not None else base.password,
            database=database or base.database,
        )

    @classmethod
    def from_env_or_partial(cls) -> "ServerConnectionConfig":
        """Like from_env but tolerant of missing vars (fills with empty strings).

        Intended as a base that CLI flags overlay on top of — missing values
        only become an error at connect time.

        If ``MYSQL_DATABASES`` is a comma-separated list, the first entry is
        used as the base ``database`` value; use :meth:`databases_from_env`
        to get the full list.
        """
        raw_db = os.environ.get("MYSQL_DATABASES", "")
        first_db = raw_db.split(",", 1)[0].strip() if raw_db else ""
        return cls(
            host=os.environ.get("MYSQL_HOST", ""),
            port=int(os.environ.get("MYSQL_PORT", "3306")),
            user=os.environ.get("MYSQL_USER", ""),
            password=os.environ.get("MYSQL_PASSWORD", ""),
            database=first_db,
        )

    @staticmethod
    def databases_from_env() -> list[str]:
        """Parse ``MYSQL_DATABASES`` as a comma-separated list of databases.

        Accepts ``foo`` (single DB) or ``foo,bar,baz`` (multi-DB). Empty /
        whitespace entries are dropped. Returns ``[]`` if the var is unset —
        callers should fall back to CLI flags in that case.
        """
        raw = os.environ.get("MYSQL_DATABASES", "")
        return [d.strip() for d in raw.split(",") if d.strip()]

    def validate(self) -> None:
        """Raise if any required field is empty. Call before connecting."""
        missing = [f for f in ("host", "user", "database") if not getattr(self, f)]
        if missing:
            raise RuntimeError(
                f"Incomplete connection config; missing: {', '.join(missing)}. "
                "Provide via --flags or MYSQL_* env vars (see .env.example)."
            )

    def to_safe_dict(self) -> dict[str, str | int]:
        """Redact password. Use this for anything that hits a log."""
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": "***REDACTED***",
            "database": self.database,
        }

    def duckdb_attach_sql(self, alias: str = "src") -> str:
        """DuckDB `ATTACH` statement for the MySQL extension.

        Uses keyword form so special characters in the password don't break
        parsing (DuckDB handles the escaping).
        """
        return (
            f"ATTACH 'host={self.host} "
            f"port={self.port} "
            f"user={self.user} "
            f"password={self.password} "
            f"database={self.database}' "
            f"AS {alias} (TYPE mysql, READ_ONLY);"
        )
