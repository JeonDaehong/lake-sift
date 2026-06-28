"""Lazy optional-dependency import helper shared by the source adapters.

Iceberg/Delta support ships behind pip extras, so the adapters import their backend
only when actually used. This centralizes that check and the "how to install it"
error message.
"""

from __future__ import annotations

import importlib


def require(module: str, extra: str) -> None:
    """Ensure optional dependency `module` is importable, else raise a helpful error.

    `module` is the import name (e.g. "pyiceberg"); `extra` is the pip extra that
    installs it (e.g. "iceberg" -> `pip install "lake-sift[iceberg]"`).
    """
    try:
        importlib.import_module(module)
    except ImportError as e:  # pragma: no cover - only without the optional dep installed
        raise ImportError(
            f'The {extra.capitalize()} source requires {module}: '
            f'pip install "lake-sift[{extra}]"'
        ) from e
