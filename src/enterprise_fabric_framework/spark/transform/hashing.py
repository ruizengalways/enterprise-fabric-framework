"""Schema-aware Spark Column expressions for distributed business-row hashing.

Canonical null, timestamp, decimal, binary and nested-value behavior must be specified before
implementation. This module must never collect rows to calculate a hash in Python.
"""

from __future__ import annotations

from typing import Any, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
else:
    Column = Any
    DataFrame = Any


def canonical_content_hash(
    dataframe: DataFrame,
    columns: Sequence[str],
    hash_ref: str,
) -> Column:
    """Return a schema-aware Spark Column for the registered canonical content hash."""

    raise NotImplementedError


__all__ = ["canonical_content_hash"]
