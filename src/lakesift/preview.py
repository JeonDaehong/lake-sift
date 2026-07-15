"""Metadata-only diff preview — what *could* have changed, without reading data.

A value diff has to read both sides. But a lakehouse table already knows most of the
answer: its data files are **immutable**, and snapshots share them. So a file present on
both sides — same path, same delete files — is provably yielding identical rows on both
sides, and can be excluded without opening it. Everything that differs is confined to the
files only one side has:

    real diff  ⊆  rows in (left-only files ∪ right-only files)

That is a *sound upper bound*: never a false negative. It is what makes this useful as a
gate — "nothing differs" is a proof, not a guess.

Per-column bounds in the manifests tighten it further. A left-only file whose **key**
range does not overlap any right-only file cannot share a key with the other side, so its
rows are pure removals — no cell of theirs can have "changed". Mirror for additions. When
*nothing* overlaps, the change is provably a pure append/delete: not one existing row was
touched. (That is the everyday shape of a date-partitioned table.)

What bounds cannot do: prove a column *unchanged*. Bounds are aggregates — values can
permute inside an unchanged range. Only claims this module can actually prove are made.

The key-range proofs assume **keys are unique**, which is what `diff()` itself enforces
(duplicate keys are an error unless `allow_duplicates`). It is what lets us say a key
found in a left-only file is not also hiding in a shared file: if it were, it would appear
twice on the right, which is precisely the duplicate `diff()` rejects.

Costs a few manifest reads (milliseconds); reads no data files at all.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Iterable, Sequence

import duckdb

from lakesift.core import DiffError, _probe_schema, _schema_changes
from lakesift.result import SchemaChange
from lakesift.sources.base import DataFileStats

if TYPE_CHECKING:
    from lakesift.sources.base import Source

# Above this many (left-only x right-only) file pairs, fall back from exact pairwise
# overlap to comparing against each side's *union* range. The union is wider, so it
# reports overlap at least as often: less tight, still sound.
_PAIRWISE_LIMIT = 250_000


def _rows(files: Iterable[DataFileStats]) -> int:
    return sum(f.record_count for f in files)


def _bytes(files: Iterable[DataFileStats]) -> int:
    return sum(f.size_bytes for f in files)


def _disjoint(a: DataFileStats, b: DataFileStats, col: str) -> bool:
    """True when a's and b's ranges for `col` provably cannot share a value.

    A missing bound means "unknown", and an incomparable pair (mixed types after schema
    evolution) means "we can't tell" — both answer False, i.e. assume they might overlap.
    Guessing "overlap" only ever widens the reported blast radius; guessing "disjoint"
    would drop real rows from it.
    """
    try:
        au, bl = a.upper_bounds.get(col), b.lower_bounds.get(col)
        if au is not None and bl is not None and au < bl:
            return True
        al, bu = a.lower_bounds.get(col), b.upper_bounds.get(col)
        if bu is not None and al is not None and bu < al:
            return True
    except TypeError:
        return False
    return False


def _may_share_key(a: DataFileStats, b: DataFileStats, key: Sequence[str]) -> bool:
    """True unless some key column's ranges are disjoint.

    One disjoint key column is enough to prove no row of `a` shares a (composite) key with
    any row of `b` — they already differ in that column.
    """
    return not any(_disjoint(a, b, k) for k in key)


def _union(files: Sequence[DataFileStats], key: Sequence[str]) -> DataFileStats:
    """A synthetic file spanning every key range in `files` (for the large-input path)."""
    lower: dict[str, Any] = {}
    upper: dict[str, Any] = {}
    for k in key:
        lows = [f.lower_bounds[k] for f in files if k in f.lower_bounds]
        ups = [f.upper_bounds[k] for f in files if k in f.upper_bounds]
        # A file missing this bound is unbounded, so the union is too -> leave it out.
        if lows and len(lows) == len(files):
            try:
                lower[k] = min(lows)
            except TypeError:
                pass
        if ups and len(ups) == len(files):
            try:
                upper[k] = max(ups)
            except TypeError:
                pass
    return DataFileStats(path="", record_count=0, size_bytes=0, lower_bounds=lower, upper_bounds=upper)


def _split_by_overlap(
    lonly: Sequence[DataFileStats], ronly: Sequence[DataFileStats], key: Sequence[str]
) -> tuple[list[DataFileStats], list[DataFileStats]]:
    """Partition each side's files into those that may share a key with the other side.

    Returns `(left_matched, right_matched)`. Whatever is left out is provably unmatched:
    removed (left) or added (right).
    """
    if not lonly or not ronly:
        return [], []  # nothing on one side -> nothing can match
    if len(lonly) * len(ronly) <= _PAIRWISE_LIMIT:
        return (
            [f for f in lonly if any(_may_share_key(f, g, key) for g in ronly)],
            [g for g in ronly if any(_may_share_key(g, f, key) for f in lonly)],
        )
    lu, ru = _union(lonly, key), _union(ronly, key)
    return (
        [f for f in lonly if _may_share_key(f, ru, key)],
        [g for g in ronly if _may_share_key(g, lu, key)],
    )


def _partitions(files: Iterable[DataFileStats]) -> list[dict[str, Any]]:
    """Distinct partition values across `files`, order preserved (empty for unpartitioned)."""
    seen: dict[tuple, dict[str, Any]] = {}
    for f in files:
        if f.partition:
            seen.setdefault(tuple(sorted(f.partition.items(), key=lambda kv: str(kv))), f.partition)
    return list(seen.values())


@dataclass(frozen=True)
class PreviewResult:
    """Blast radius of a diff, derived from table metadata alone.

    Every row count here is a **bound**, not a measurement: `provably_added` /
    `provably_removed` are lower bounds on what a real diff would report, and
    `max_changed_rows` is an upper bound on the rows whose cells can have changed. Run the
    real `diff()` for exact numbers — `rows_to_scan` says how much it would cost.
    """

    key: list[str] = field(default_factory=list)
    schema_changes: list[SchemaChange] = field(default_factory=list)

    # --- file sets (the proof) ---
    files_left: int = 0
    files_right: int = 0
    files_shared: int = 0  # identical on both sides -> excluded from any scan

    # --- what a diff would have to read ---
    rows_left: int = 0
    rows_right: int = 0
    rows_to_scan: int = 0
    bytes_left: int = 0
    bytes_right: int = 0
    bytes_to_scan: int = 0

    # --- key-range proofs (only when a key was given) ---
    key_pruned: bool = False
    provably_added: int = 0
    provably_removed: int = 0
    max_changed_rows: int = 0

    partitions_touched: list[dict[str, Any]] = field(default_factory=list)
    # Merge-on-read deletes seen. Handled (a delete file is part of a data file's
    # identity), but worth surfacing: they make shared files rarer.
    has_deletes: bool = False

    @property
    def files_differing(self) -> int:
        return (self.files_left + self.files_right) - 2 * self.files_shared

    @property
    def rows_total(self) -> int:
        """Rows a full diff would read (both sides)."""
        return self.rows_left + self.rows_right

    @property
    def bytes_total(self) -> int:
        return self.bytes_left + self.bytes_right

    @property
    def scan_fraction(self) -> float:
        """Share of the full scan a pruned diff still has to read (0.0 – 1.0)."""
        return self.rows_to_scan / self.rows_total if self.rows_total else 0.0

    def is_empty(self) -> bool:
        """True when the two sides are *provably* identical — no file differs at all.

        The strong direction of the proof: no data read, yet certain. The converse does
        not hold — differing files may still hold identical rows (a compaction rewrites
        files without changing a value), so a non-empty preview means "may differ".
        """
        return not self.schema_changes and self.files_differing == 0

    def is_pure_append(self) -> bool:
        """True when no existing row can have been modified — proven from key ranges.

        Every differing file is provably unmatched on the other side, so the change is
        additions and/or removals only.
        """
        return self.key_pruned and not self.is_empty() and self.max_changed_rows == 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "schema_changes": [
                {"column": c.column, "kind": c.kind, "old_type": c.old_type, "new_type": c.new_type}
                for c in self.schema_changes
            ],
            "files": {
                "left": self.files_left,
                "right": self.files_right,
                "shared": self.files_shared,
                "differing": self.files_differing,
            },
            "scan": {
                "rows_to_scan": self.rows_to_scan,
                "rows_total": self.rows_total,
                "bytes_to_scan": self.bytes_to_scan,
                "bytes_total": self.bytes_total,
                "fraction": self.scan_fraction,
            },
            "bounds": {
                "key_pruned": self.key_pruned,
                "provably_added": self.provably_added,
                "provably_removed": self.provably_removed,
                "max_changed_rows": self.max_changed_rows,
                "pure_append": self.is_pure_append(),
            },
            "partitions_touched": self.partitions_touched,
            "has_deletes": self.has_deletes,
            "identical": self.is_empty(),
        }

    def to_json(self, *, indent: int | None = 2) -> str:
        import json

        return json.dumps(self.to_dict(), default=str, ensure_ascii=False, indent=indent)


def preview(left: "Source", right: "Source", *, key: Sequence[str] | None = None) -> PreviewResult:
    """Estimate a diff's blast radius from metadata only — no data is read.

    Answers "is it worth running the real diff, and what would it cost?" in the time it
    takes to read a few manifests. Both sources must expose `data_files()` (Iceberg
    today); pass `key` to also get the key-range proofs (pure-append detection and exact
    added/removed counts for unmatched files).

    Most useful on two snapshots/branches of the *same* table, where file sharing is high.
    It stays correct on unrelated tables — they simply share no files, so the preview
    reports that a full scan is needed.

    Raises:
        DiffError: a source cannot describe its data files.
    """
    key = list(key or [])
    lfiles, rfiles = _data_files(left, "left"), _data_files(right, "right")

    # --- the proof: a file on both sides (same bytes, same deletes) is identical ---
    lids = {f.identity for f in lfiles}
    rids = {f.identity for f in rfiles}
    shared = lids & rids
    lonly = [f for f in lfiles if f.identity not in rids]
    ronly = [f for f in rfiles if f.identity not in lids]

    # --- key-range proofs over what is left ---
    l_matched, r_matched = _split_by_overlap(lonly, ronly, key) if key else (lonly, ronly)

    con = duckdb.connect()  # only used to normalize schema types; closed right away
    try:
        schema_changes = _schema_changes(_probe_schema(con, left), _probe_schema(con, right))
    finally:
        con.close()

    return PreviewResult(
        key=key,
        schema_changes=schema_changes,
        files_left=len(lfiles),
        files_right=len(rfiles),
        files_shared=len(shared),
        rows_left=_rows(lfiles),
        rows_right=_rows(rfiles),
        rows_to_scan=_rows(lonly) + _rows(ronly),
        bytes_left=_bytes(lfiles),
        bytes_right=_bytes(rfiles),
        bytes_to_scan=_bytes(lonly) + _bytes(ronly),
        key_pruned=bool(key),
        # Rows in files that match nothing on the other side cannot be "changed" rows.
        provably_removed=_rows(lonly) - _rows(l_matched) if key else 0,
        provably_added=_rows(ronly) - _rows(r_matched) if key else 0,
        # A changed row needs its key on both sides, so it must live in a matched file on
        # *each* side — whichever side has fewer such rows caps the count.
        max_changed_rows=min(_rows(l_matched), _rows(r_matched)) if key else 0,
        partitions_touched=_partitions(lonly + ronly),
        has_deletes=any(f.delete_files for f in lfiles + rfiles),
    )


def _data_files(source: "Source", side: str) -> list[DataFileStats]:
    fn = getattr(source, "data_files", None)
    if fn is None:
        raise DiffError(
            f"--preview needs a source that describes its data files; the {side} side "
            f"({type(source).__name__}) does not. Iceberg sources do."
        )
    return list(fn())
