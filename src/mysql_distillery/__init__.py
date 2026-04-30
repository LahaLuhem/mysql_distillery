"""Component-wise MySQL snapshot extractor.

Each submodule under `components/` is independently runnable and writes a
single kind of artifact (schema, data, views, ...) into an output directory.
`extract.py` is the orchestrator that runs them in parallel.
"""

__version__ = "0.1.0"
