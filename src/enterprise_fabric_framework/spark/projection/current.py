"""Spark-native projection boundary for derived current-state relations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from enterprise_fabric_framework.contracts import FrozenSilverPlan

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
else:
    DataFrame = Any


@dataclass(slots=True)
class ProjectionResult:
    dataframe: DataFrame
    evidence_ref: str | None = None


def project_current_state(
    dataframe: DataFrame,
    plan: FrozenSilverPlan,
) -> ProjectionResult:
    """Build a current-state projection from an authoritative Spark/Delta history."""

    raise NotImplementedError


__all__ = ["ProjectionResult", "project_current_state"]
