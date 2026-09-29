"""Bronze-writer contract for append-only Spark/Delta publication."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, TYPE_CHECKING

from enterprise_fabric_framework.contracts import (
    BronzeManifestEvidence,
    FrozenBronzePlan,
    SourceReadEvidence,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
else:
    DataFrame = Any


@dataclass(slots=True)
class BronzeWriteRequest:
    bronze_run_id: str
    dataframe: DataFrame
    plan: FrozenBronzePlan
    source_evidence: SourceReadEvidence


@dataclass(frozen=True, slots=True)
class BronzeWriteResult:
    manifest: BronzeManifestEvidence


class BronzeWriter(Protocol):
    """Registered implementation that appends one Bronze delivery."""

    def write(self, request: BronzeWriteRequest) -> BronzeWriteResult:
        """Publish an append-only Bronze delivery."""

        ...


def write_bronze(request: BronzeWriteRequest, writer: BronzeWriter) -> BronzeWriteResult:
    """Invoke a registered Bronze writer."""

    raise NotImplementedError


__all__ = ["BronzeWriteRequest", "BronzeWriteResult", "BronzeWriter", "write_bronze"]
