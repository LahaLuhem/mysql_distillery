"""Entry point: `python src/main.py ...` → orchestrator CLI.

The installed console script `mysql_distillery` (see pyproject.toml) is the
preferred invocation; this file exists for in-tree runs without an install step.
"""
from mysql_distillery.extract import cli

if __name__ == "__main__":
    cli()
