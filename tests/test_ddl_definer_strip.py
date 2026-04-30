"""Tests for strip_definer against view / routine / trigger / event DDL."""
from __future__ import annotations

from mysql_distillery.data.utils.ddl import strip_definer


def test_view_definer_removed():
    ddl = (
        "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` "
        "SQL SECURITY DEFINER VIEW `active_users` AS "
        "SELECT * FROM users WHERE active = 1"
    )
    out = strip_definer(ddl)
    # The DEFINER=... assignment must be gone ...
    assert "DEFINER=" not in out
    assert "`root`@`localhost`" not in out
    # ... but the `SQL SECURITY DEFINER` clause is a different thing
    # (it controls whose privileges the view runs with) and must survive.
    assert "SQL SECURITY DEFINER" in out
    assert "VIEW `active_users`" in out


def test_procedure_definer_removed():
    ddl = (
        "CREATE DEFINER=`app`@`%` PROCEDURE `recalc_totals`(IN user_id INT)\n"
        "BEGIN\n"
        "  UPDATE totals SET t = 0 WHERE uid = user_id;\n"
        "END"
    )
    out = strip_definer(ddl)
    assert "DEFINER" not in out
    assert "PROCEDURE `recalc_totals`" in out


def test_trigger_definer_removed():
    ddl = (
        "CREATE DEFINER=`root`@`%` TRIGGER `orders_bi` BEFORE INSERT ON `orders` "
        "FOR EACH ROW SET NEW.created_at = NOW()"
    )
    out = strip_definer(ddl)
    assert "DEFINER" not in out
    assert "TRIGGER `orders_bi`" in out


def test_event_definer_removed():
    ddl = (
        "CREATE DEFINER=`admin`@`%` EVENT `nightly_cleanup` "
        "ON SCHEDULE EVERY 1 DAY STARTS '2026-01-01 02:00:00' "
        "DO DELETE FROM sessions WHERE expires_at < NOW()"
    )
    out = strip_definer(ddl)
    assert "DEFINER" not in out
    assert "EVENT `nightly_cleanup`" in out


def test_no_definer_is_unchanged():
    ddl = "CREATE VIEW v AS SELECT 1"
    assert strip_definer(ddl) == ddl


def test_case_insensitive():
    ddl = "CREATE definer=`u`@`h` VIEW v AS SELECT 1"
    out = strip_definer(ddl)
    assert "definer" not in out.lower()
