"""Read-only introspection of the server's data connections — no secrets, ever.

lake-sift never stores or manages credentials; it inherits them from the environment the
server runs in (AWS_* vars, a shared profile, an instance role, `~/.pyiceberg.yaml`, …).
This module reports *what is configured* so the UI can tell the user which sources they can
reference — catalog names, storage backends, whether credentials are present — without
reading or displaying a single secret value.

Everything here is best-effort and defensive: a missing optional dep, an unreadable config
file, or an offline DuckDB must degrade to "unknown", never raise.
"""

from __future__ import annotations

import os
from importlib.util import find_spec
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

# status ∈ {"ok", "warn", "off"} — drives the indicator colour in the template.


def _adapter(label: str, module: str, extra: str) -> dict[str, Any]:
    present = find_spec(module) is not None
    return {
        "label": label,
        "status": "ok" if present else "off",
        "detail": "installed" if present else f"pip install 'lake-sift[{extra}]'",
    }


def _aws_credentials() -> dict[str, Any]:
    """Detect AWS credentials *without* reading their values. Only presence + source."""
    env = os.environ
    if env.get("AWS_ACCESS_KEY_ID"):
        src = "AWS_ACCESS_KEY_ID env var"
    elif env.get("AWS_WEB_IDENTITY_TOKEN_FILE") or env.get("AWS_ROLE_ARN"):
        src = "web-identity / assumed role"
    elif env.get("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") or env.get(
        "AWS_CONTAINER_CREDENTIALS_FULL_URI"
    ):
        src = "container credentials (ECS/EKS)"
    elif env.get("AWS_PROFILE"):
        src = f"shared profile '{env['AWS_PROFILE']}'"
    elif (Path.home() / ".aws" / "credentials").exists():
        src = "~/.aws/credentials"
    else:
        return {"label": "AWS credentials", "status": "off",
                "detail": "not detected — set AWS_* env, a profile, or an instance role"}
    return {"label": "AWS credentials", "status": "ok", "detail": f"detected · {src}"}


def _aws_region() -> dict[str, Any]:
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if region:  # a region name is not a secret
        return {"label": "AWS region", "status": "ok", "detail": region}
    return {"label": "AWS region", "status": "warn", "detail": "not set (AWS_REGION)"}


def _httpfs() -> dict[str, Any]:
    """Can DuckDB load httpfs? Needed for s3:// and http(s):// Parquet."""
    try:
        import duckdb

        con = duckdb.connect()
        try:
            con.execute("INSTALL httpfs")
            con.execute("LOAD httpfs")
        finally:
            con.close()
        return {"label": "DuckDB httpfs", "status": "ok", "detail": "loadable (s3://, http(s):// Parquet)"}
    except Exception as e:
        return {"label": "DuckDB httpfs", "status": "warn",
                "detail": f"could not load ({type(e).__name__}) — remote Parquet may be offline"}


def _redact_uri(uri: str) -> str:
    """Strip any embedded credentials from a URI, keeping scheme://host[/path]."""
    try:
        parts = urlsplit(uri)
        host = parts.hostname or ""
        if parts.port:
            host = f"{host}:{parts.port}"
        return urlunsplit((parts.scheme, host, parts.path, "", "")) or uri
    except Exception:
        return "(uri hidden)"


def _pyiceberg_config_path() -> Path | None:
    home = os.environ.get("PYICEBERG_HOME")
    base = Path(home) if home else Path.home()
    path = base / ".pyiceberg.yaml"
    return path if path.exists() else None


# Config keys safe to surface. Anything else in a catalog block (access keys, tokens,
# passwords) is deliberately never read out.
_SAFE_CATALOG_KEYS = {"type", "uri", "warehouse"}


def _iceberg_catalogs() -> dict[str, Any]:
    """List Iceberg catalog *names/types* from ~/.pyiceberg.yaml — never their secrets."""
    path = _pyiceberg_config_path()
    result: dict[str, Any] = {"config_path": str(path) if path else None, "catalogs": [], "note": None}
    # Env-defined catalogs: PYICEBERG_CATALOG__<name>__<key>
    env_names: set[str] = set()
    for k in os.environ:
        if k.startswith("PYICEBERG_CATALOG__"):
            rest = k[len("PYICEBERG_CATALOG__"):]
            env_names.add(rest.split("__", 1)[0].lower())

    if path is None and not env_names:
        result["note"] = "no ~/.pyiceberg.yaml and no PYICEBERG_CATALOG__* env — catalogs configured elsewhere will still work if reachable"
        return result

    catalogs: list[dict[str, str]] = []
    if path is not None:
        try:
            import yaml  # PyYAML ships with pyiceberg

            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            for name, cfg in (data.get("catalog") or {}).items():
                cfg = cfg or {}
                entry = {"name": name, "type": str(cfg.get("type", "?"))}
                if cfg.get("uri"):
                    entry["uri"] = _redact_uri(str(cfg["uri"]))
                if cfg.get("warehouse"):
                    entry["warehouse"] = _redact_uri(str(cfg["warehouse"]))
                catalogs.append(entry)
        except ModuleNotFoundError:
            result["note"] = "found ~/.pyiceberg.yaml (install pyyaml to list catalog names)"
        except Exception:
            result["note"] = "found ~/.pyiceberg.yaml but could not parse it"

    for name in sorted(env_names):
        if not any(c["name"] == name for c in catalogs):
            catalogs.append({"name": name, "type": "env"})

    result["catalogs"] = catalogs
    return result


def probe_environment(history_db: str) -> dict[str, Any]:
    """Assemble the full read-only environment report for the /environment page."""
    from lakesift import __version__

    return {
        "version": __version__,
        "history_db": history_db,
        "adapters": [
            _adapter("Iceberg (pyiceberg)", "pyiceberg", "iceberg"),
            _adapter("Delta (deltalake)", "deltalake", "delta"),
            _adapter("SQL schema predict (sqlglot)", "sqlglot", "sql"),
        ],
        "storage": [_aws_credentials(), _aws_region(), _httpfs()],
        "iceberg": _iceberg_catalogs(),
    }
