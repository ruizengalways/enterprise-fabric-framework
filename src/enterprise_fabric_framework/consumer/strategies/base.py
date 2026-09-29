"""Silver strategy contract for applying a micro-batch to a Silver target."""

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
    """Registered implementation that applies one frozen Silver strategy."""

    def load(self, request: SilverLoadRequest) -> SilverLoadResult:
        """Apply one micro-batch to the Silver target."""

        ...


def execute_silver_load(
    request: SilverLoadRequest,
    executor: SilverLoadExecutor,
) -> SilverLoadResult:
    """Apply a registered Silver strategy."""

    raise NotImplementedError


__all__ = ["SilverLoadExecutor", "SilverLoadRequest", "SilverLoadResult", "execute_silver_load"]
