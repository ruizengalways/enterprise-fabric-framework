"""Spark-native reconciliation observations with bounded driver summaries."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence, TYPE_CHECKING

from enterprise_fabric_framework.contracts import (
    BronzeManifestEvidence,
    RuleResult,
    RuleSpec,
    SilverBatchEvidence,
    SourceReadEvidence,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
else:
    DataFrame = Any


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    passed: bool
    rule_results: tuple[RuleResult, ...]
    evidence_ref: str | None = None


def reconcile_bronze(
    source_evidence: SourceReadEvidence,
    bronze_evidence: BronzeManifestEvidence,
    rules: Sequence[RuleSpec],
    *,
    bronze_run_id: str,
) -> ReconciliationResult:
    """Evaluate Source-to-Bronze publication and completeness rules."""

    raise NotImplementedError


def reconcile_silver(
    input_dataframe: DataFrame,
    silver_evidence: SilverBatchEvidence,
    rules: Sequence[RuleSpec],
    *,
    silver_run_id: str,
) -> ReconciliationResult:
    """Evaluate Bronze-to-Silver batch accounting and target reconciliation rules."""

    raise NotImplementedError


__all__ = ["ReconciliationResult", "reconcile_bronze", "reconcile_silver"]
