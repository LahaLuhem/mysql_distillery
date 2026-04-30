"""Tests for strip_foreign_keys against realistic MySQL 8 SHOW CREATE TABLE output."""
from __future__ import annotations

from mysql_distillery.data.utils.ddl import strip_foreign_keys


def test_no_foreign_keys_is_unchanged():
    ddl = (
        "CREATE TABLE `users` (\n"
        "  `id` int NOT NULL AUTO_INCREMENT,\n"
        "  `email` varchar(255) NOT NULL,\n"
        "  PRIMARY KEY (`id`),\n"
        "  UNIQUE KEY `email` (`email`)\n"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    )
    assert strip_foreign_keys(ddl) == ddl


def test_single_foreign_key_at_end_removed():
    ddl = (
        "CREATE TABLE `orders` (\n"
        "  `id` int NOT NULL AUTO_INCREMENT,\n"
        "  `user_id` int NOT NULL,\n"
        "  PRIMARY KEY (`id`),\n"
        "  KEY `idx_user_id` (`user_id`),\n"
        "  CONSTRAINT `fk_orders_user` FOREIGN KEY (`user_id`) "
        "REFERENCES `users` (`id`) ON DELETE CASCADE ON UPDATE CASCADE\n"
        ") ENGINE=InnoDB"
    )
    out = strip_foreign_keys(ddl)
    assert "FOREIGN KEY" not in out
    assert "CONSTRAINT `fk_orders_user`" not in out
    # Indexes and primary key must survive.
    assert "PRIMARY KEY (`id`)" in out
    assert "KEY `idx_user_id` (`user_id`)" in out
    # Closing parens must be intact, no dangling comma before ENGINE.
    assert ") ENGINE=InnoDB" in out
    assert ",\n)" not in out


def test_multiple_foreign_keys_all_removed():
    ddl = (
        "CREATE TABLE `order_items` (\n"
        "  `id` int NOT NULL AUTO_INCREMENT,\n"
        "  `order_id` int NOT NULL,\n"
        "  `product_id` int NOT NULL,\n"
        "  PRIMARY KEY (`id`),\n"
        "  CONSTRAINT `fk_oi_order` FOREIGN KEY (`order_id`) "
        "REFERENCES `orders` (`id`) ON DELETE CASCADE,\n"
        "  CONSTRAINT `fk_oi_product` FOREIGN KEY (`product_id`) "
        "REFERENCES `products` (`id`)\n"
        ") ENGINE=InnoDB"
    )
    out = strip_foreign_keys(ddl)
    assert "FOREIGN KEY" not in out
    assert "`fk_oi_order`" not in out
    assert "`fk_oi_product`" not in out
    assert "PRIMARY KEY (`id`)" in out
    assert ") ENGINE=InnoDB" in out
    # No stray commas left over from removed foreign_keys.
    assert ",\n)" not in out
    assert ",\n  )" not in out


def test_foreign_key_without_on_clauses_removed():
    ddl = (
        "CREATE TABLE `a` (\n"
        "  `b_id` int NOT NULL,\n"
        "  PRIMARY KEY (`b_id`),\n"
        "  CONSTRAINT `fk_a_b` FOREIGN KEY (`b_id`) REFERENCES `b` (`id`)\n"
        ") ENGINE=InnoDB"
    )
    out = strip_foreign_keys(ddl)
    assert "FOREIGN KEY" not in out


def test_composite_foreign_key_removed():
    ddl = (
        "CREATE TABLE `link` (\n"
        "  `a_id` int NOT NULL,\n"
        "  `b_id` int NOT NULL,\n"
        "  PRIMARY KEY (`a_id`,`b_id`),\n"
        "  CONSTRAINT `fk_link` FOREIGN KEY (`a_id`, `b_id`) "
        "REFERENCES `other` (`x`, `y`) ON DELETE SET NULL\n"
        ") ENGINE=InnoDB"
    )
    out = strip_foreign_keys(ddl)
    assert "FOREIGN KEY" not in out
    assert "PRIMARY KEY (`a_id`,`b_id`)" in out


def test_dangling_comma_cleaned_up():
    # foreign_key was the only thing after the last column, so removing it leaves a
    # trailing comma before `)`.
    ddl = (
        "CREATE TABLE `t` (\n"
        "  `id` int NOT NULL,\n"
        "  CONSTRAINT `fk` FOREIGN KEY (`id`) REFERENCES `u` (`id`)\n"
        ") ENGINE=InnoDB"
    )
    out = strip_foreign_keys(ddl)
    # The comma at the end of `id int NOT NULL,` must be cleaned up.
    assert ",\n)" not in out
    assert "`id` int NOT NULL\n) ENGINE=InnoDB" in out
