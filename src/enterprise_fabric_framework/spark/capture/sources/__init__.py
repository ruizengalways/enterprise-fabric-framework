"""Registered Spark-native source readers.

Readers obtain Spark DataFrames from explicit frozen source boundaries. They do not choose the
Bronze representation, mutate Silver targets or return business rows to Python.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, TYPE_CHECKING

from enterprise_fabric_framework.contracts import FrozenBronzePlan, SourceReadBoundary, SourceReadEvidence

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
else:
    DataFrame = Any


@dataclass(frozen=True, slots=True)
class SourceReadRequest:
    bronze_run_id: str
    plan: FrozenBronzePlan
    boundary: SourceReadBoundary


@dataclass(slots=True)
class SourceReadResult:
    dataframe: DataFrame
    evidence: SourceReadEvidence


class SourceReader(Protocol):
    """Registered source-reader implementation boundary."""

    def read(self, request: SourceReadRequest) -> SourceReadResult:
        """Read source facts into a Spark DataFrame."""

        ...


def read_source(request: SourceReadRequest, reader: SourceReader) -> SourceReadResult:
    """Invoke the registered source reader."""

    raise NotImplementedError


__all__ = ["SourceReadRequest", "SourceReadResult", "SourceReader", "read_source"]
