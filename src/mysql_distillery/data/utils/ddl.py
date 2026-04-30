"""Regex-based DDL cleanup helpers — extracted here so they're easy to unit-test."""
from __future__ import annotations

import re

# MySQL 8 canonical `SHOW CREATE TABLE` emits foreign_key lines of the form:
#   ,\n  CONSTRAINT `foreign_key_name` FOREIGN KEY (`col`[, `col2`]) REFERENCES `tbl` (`col`)
#     [ON DELETE {CASCADE|SET NULL|NO ACTION|RESTRICT|SET DEFAULT}]
#     [ON UPDATE {CASCADE|SET NULL|NO ACTION|RESTRICT|SET DEFAULT}]
# The leading comma varies; the trailing comma may or may not be present if the
# foreign_key is the last column/constraint line. We match either form and clean up.
_FOREIGN_KEY_CLAUSE_RE = re.compile(
    r"""
    ,?                                      # optional leading comma
    [ \t]*\n[ \t]*                          # newline + indentation
    CONSTRAINT[ \t]+`[^`]+`[ \t]+
    FOREIGN[ \t]+KEY[ \t]*\([^)]+\)[ \t]*
    REFERENCES[ \t]+`[^`]+`[ \t]*\([^)]+\)
    (?:[ \t]+ON[ \t]+DELETE[ \t]+(?:CASCADE|SET[ \t]+NULL|NO[ \t]+ACTION|RESTRICT|SET[ \t]+DEFAULT))?
    (?:[ \t]+ON[ \t]+UPDATE[ \t]+(?:CASCADE|SET[ \t]+NULL|NO[ \t]+ACTION|RESTRICT|SET[ \t]+DEFAULT))?
    """,
    re.VERBOSE | re.IGNORECASE,
)

# DEFINER=`user`@`host`
_DEFINER_RE = re.compile(r"\s*DEFINER\s*=\s*`[^`]+`@`[^`]+`", re.IGNORECASE)

# A "dangling" trailing comma right before a closing ) on a constraints block
# can be left behind after foreign_key removal. Clean it up.
_DANGLING_COMMA_RE = re.compile(r",(\s*\n\s*\))")


def strip_foreign_keys(create_table_sql: str) -> str:
    """Remove `CONSTRAINT ... FOREIGN KEY ...` clauses from a CREATE TABLE.

    Indexes and PRIMARY/UNIQUE KEY lines are untouched. Returns the cleaned
    DDL. Actual foreign_key recreation is emitted separately by `components/constraints.py`.
    """
    cleaned = _FOREIGN_KEY_CLAUSE_RE.sub("", create_table_sql)
    cleaned = _DANGLING_COMMA_RE.sub(r"\1", cleaned)
    return cleaned


def strip_definer(ddl: str) -> str:
    """Remove `DEFINER=`user`@`host`` clauses from views/routines/triggers/events.

    Leaving DEFINER in place makes restores fail on any host that doesn't have
    the exact same user. Stripping it lets MySQL default DEFINER to the invoker.
    """
    return _DEFINER_RE.sub("", ddl)
