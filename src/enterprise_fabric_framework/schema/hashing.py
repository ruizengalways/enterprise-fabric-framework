"""Schema-aware Spark Column expressions for distributed content hashing."""

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
    """Return a Spark Column for the selected canonical content hash."""

    raise NotImplementedError


__all__ = ["canonical_content_hash"]
