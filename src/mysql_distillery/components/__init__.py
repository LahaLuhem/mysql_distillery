"""Per-component extractors.

Each module here exports:
    run(cfg: ServerConnectionConfig, out_dir: Path, *, max_workers: int = 1) -> ComponentResult

…and exposes a standalone CLI when invoked as `python -m mysql_distillery.components.<name>`.
"""
