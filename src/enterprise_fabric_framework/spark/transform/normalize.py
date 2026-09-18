"""Spark-native normalization boundary between captured Bronze facts and Silver loading."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from enterprise_fabric_framework.contracts import FrozenSilverPlan

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
else:
    DataFrame = Any


@dataclass(slots=True)
class TransformResult:
    dataframe: DataFrame
    evidence_ref: str | None = None


def normalize_bronze(
    dataframe: DataFrame,
    plan: FrozenSilverPlan,
) -> TransformResult:
    """Normalize retained Bronze facts using the frozen Silver plan."""

    raise NotImplementedError


__all__ = ["TransformResult", "normalize_bronze"]
