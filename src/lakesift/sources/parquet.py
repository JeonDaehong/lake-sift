"""Parquet source adapter (v0).

Local paths work with no setup. Remote paths (``s3://``, ``gs://``, ``http(s)://`` …) are
read through DuckDB's ``httpfs`` extension, which this adapter loads on demand; for S3 it
also wires up a credential-chain secret so the standard AWS environment (``AWS_*`` vars, a
shared profile, SSO, or an instance/IRSA role) is picked up automatically. No credentials
are ever read or stored by lake-sift itself — they stay in the environment DuckDB inherits.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Sequence

from lakesift._sql import quote_identifier as _q

if TYPE_CHECKING:
    import duckdb

# Schemes that are served over the network and need the httpfs extension. `s3a://` is a
# Hadoop-ism DuckDB doesn't grok, but people paste it; normalize it to `s3://` below.
_REMOTE_SCHEMES = ("s3://", "s3a://", "gs://", "gcs://", "r2://", "http://", "https://")
_S3_SCHEMES = ("s3://", "s3a://", "gs://", "gcs://", "r2://")


def _is_remote(path: str) -> bool:
    return path.startswith(_REMOTE_SCHEMES)


def configure_remote_io(con: "duckdb.DuckDBPyConnection", path: str) -> None:
    """Prepare `con` to read a remote `path`, if it is one (no-op for local paths).

    Loads httpfs and, for S3-style URLs, registers a credential-chain secret so DuckDB
    authenticates the same way the AWS SDK would. Best-effort and idempotent: any failure
    is left for the actual read to surface with a real error message.
    """
    if not _is_remote(path):
        return
    try:
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
    except Exception:
        # Offline or extension unavailable — let read_parquet raise the concrete error.
        return
    if path.startswith(_S3_SCHEMES):
        # credential_chain covers AWS_* env vars, shared config/credentials, SSO, and
        # instance/IRSA roles — the same resolution order as the AWS SDK.
        try:
            con.execute(
                "CREATE SECRET lakesift_s3 (TYPE s3, PROVIDER credential_chain)"
            )
        except Exception:
            # Older DuckDB without credential_chain: fall back to explicit AWS_* env.
            _s3_secret_from_env(con)


def _s3_secret_from_env(con: "duckdb.DuckDBPyConnection") -> None:
    key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not key or not secret:
        return  # nothing to configure; an anonymous/public bucket may still work
    parts = [f"KEY_ID '{_esc(key)}'", f"SECRET '{_esc(secret)}'"]
    token = os.environ.get("AWS_SESSION_TOKEN")
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if token:
        parts.append(f"SESSION_TOKEN '{_esc(token)}'")
    if region:
        parts.append(f"REGION '{_esc(region)}'")
    try:
        con.execute(f"CREATE SECRET lakesift_s3 (TYPE s3, {', '.join(parts)})")
    except Exception:
        pass


def _esc(value: str) -> str:
    return value.replace("'", "''")


class ParquetSource:
    """Reads a single Parquet file (or glob) into a DuckDB relation."""

    def __init__(self, path: str | os.PathLike[str]):
        self.path = os.fspath(path)

    def to_relation(
        self,
        con: "duckdb.DuckDBPyConnection",
        *,
        columns: Sequence[str] | None = None,
    ) -> "duckdb.DuckDBPyRelation":
        configure_remote_io(con, self.path)
        # Inject the path via parameter binding (avoids SQL injection/quoting issues).
        select = "*" if columns is None else ", ".join(_q(c) for c in columns)
        return con.from_query(f"SELECT {select} FROM read_parquet(?)", params=[self.path])

    def __repr__(self) -> str:  # pragma: no cover
        return f"ParquetSource({self.path!r})"
