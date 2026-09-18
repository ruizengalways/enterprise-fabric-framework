"""Registered Spark/Delta APPEND, REPLACE, UPSERT, SCD1, SCD2 and SNAPSHOT_DIFF executors.

DatasetRunner resolves these internal strategies from a frozen typed Silver run. Pipelines and
domain code do not instantiate them directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, TYPE_CHECKING

from enterprise_fabric_framework.contracts import FrozenSilverPlan, SilverBatchEvidence

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
else:
    DataFrame = Any


@dataclass(slots=True)
class SilverLoadRequest:
    silver_run_id: str
    batch_id: int
    dataframe: DataFrame
    plan: FrozenSilverPlan


@dataclass(frozen=True, slots=True)
class SilverLoadResult:
    evidence: SilverBatchEvidence


class SilverLoadExecutor(Protocol):
    """Registered Silver mutation strategy boundary."""

    def load(self, request: SilverLoadRequest) -> SilverLoadResult:
        """Apply one frozen APPEND/REPLACE/UPSERT/SCD/SNAPSHOT strategy."""

        ...


def execute_silver_load(
    request: SilverLoadRequest,
    executor: SilverLoadExecutor,
) -> SilverLoadResult:
    """Invoke the registered Silver load strategy."""

    raise NotImplementedError


__all__ = [
    "SilverLoadExecutor",
    "SilverLoadRequest",
    "SilverLoadResult",
    "execute_silver_load",
]
