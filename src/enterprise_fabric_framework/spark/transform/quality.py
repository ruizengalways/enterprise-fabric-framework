"""Spark-native quality evaluation boundary.

Rules are compiled to distributed Spark expressions or aggregations.  This module never collects
business rows to Python; invalid-row details are written through the runtime's evidence boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence, TYPE_CHECKING

from enterprise_fabric_framework.contracts import RuleResult, RuleSpec

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
else:
    DataFrame = Any


@dataclass(slots=True)
class QualityEvaluation:
    valid_dataframe: DataFrame
    quarantine_dataframe: DataFrame | None
    rule_results: tuple[RuleResult, ...]


def evaluate_quality(
    dataframe: DataFrame,
    rules: Sequence[RuleSpec],
    *,
    run_id: str,
    batch_id: int | None = None,
) -> QualityEvaluation:
    """Evaluate QUALITY rules and return valid/quarantine DataFrames plus bounded results."""

    raise NotImplementedError


__all__ = ["QualityEvaluation", "evaluate_quality"]
