"""Abstract Spark-native data-quality evaluation boundary."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, TYPE_CHECKING

from enterprise_fabric_framework.quality.results import (
    QualityRuleResult,
    QualityRunResult,
)
from enterprise_fabric_framework.quality.status import DataQualitySeverity, DataQualityStatus

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
else:
    DataFrame = Any


@dataclass(frozen=True, slots=True)
class QualityEvaluation:
    """Spark data eligible for the next step and bounded pipeline evidence."""

    valid_dataframe: DataFrame
    quarantine_dataframe: DataFrame | None
    result: QualityRunResult


class QualityFailureAction(StrEnum):
    """Configured treatment for rows that do not satisfy a quality rule."""

    REPORT = "REPORT"
    QUARANTINE = "QUARANTINE"
    FAIL = "FAIL"


@dataclass(frozen=True, slots=True)
class SparkSqlQualityRule:
    """One Spark SQL predicate evaluated against every input row.

    ``condition`` must evaluate to ``true`` for a row to pass. Null values are treated as
    failures so a rule such as ``customer_id IS NOT NULL`` has explicit null semantics.
    """

    rule_id: str
    condition: str
    failure_action: QualityFailureAction = QualityFailureAction.FAIL
    evidence_ref: str | None = None
    severity: DataQualitySeverity = DataQualitySeverity.CRITICAL

    def __post_init__(self) -> None:
        if not self.rule_id.strip():
            raise ValueError("rule_id must not be empty")
        if not self.condition.strip():
            raise ValueError("condition must not be empty")
        if (
            self.failure_action is QualityFailureAction.FAIL
            and self.severity is not DataQualitySeverity.CRITICAL
        ):
            raise ValueError("a FAIL data-quality rule must have CRITICAL severity")


class DataQualityEvaluator(ABC):
    """Extension point for one source's DQ logic.

    Implementations evaluate Spark DataFrames and return bounded evidence. They
    do not write runtime tables or make pipeline-routing decisions.
    """

    @abstractmethod
    def evaluate(self, dataframe: DataFrame, *, run_id: str) -> QualityEvaluation:
        """Evaluate configured rules and return data plus JSON-ready evidence."""

        raise NotImplementedError


class SparkSqlDataQualityEvaluator(DataQualityEvaluator):
    """Evaluate declarative quality rules without moving business rows to the driver.

    ``REPORT`` retains failed rows and allows publication. ``QUARANTINE`` removes failed rows
    from the publication candidate and returns them separately. ``FAIL`` makes the evaluation
    unsuccessful, so a caller must not publish the candidate. Subclass ``DataQualityEvaluator``
    for source-specific checks that cannot be expressed as Spark SQL predicates.
    """

    def __init__(self, rules: Sequence[SparkSqlQualityRule]) -> None:
        self._rules = tuple(rules)
        rule_ids = [rule.rule_id for rule in self._rules]
        if len(set(rule_ids)) != len(rule_ids):
            raise ValueError("quality rule IDs must be unique")

    def evaluate(self, dataframe: DataFrame, *, run_id: str) -> QualityEvaluation:
        """Evaluate the configured rules and return only distributed Spark DataFrames."""

        from pyspark.sql import functions as functions

        failed_conditions = [
            ~functions.coalesce(
                functions.expr(rule.condition).cast("boolean"),
                functions.lit(False),
            )
            for rule in self._rules
        ]
        failure_aliases = [f"rule_{index}_failed_rows" for index in range(len(self._rules))]
        valid_condition = functions.lit(True)
        quarantine_condition = functions.lit(False)
        for rule, failed_condition in zip(self._rules, failed_conditions, strict=True):
            if rule.failure_action is not QualityFailureAction.REPORT:
                valid_condition = valid_condition & ~failed_condition
            if rule.failure_action is QualityFailureAction.QUARANTINE:
                quarantine_condition = quarantine_condition | failed_condition

        aggregate_columns = [functions.count(functions.lit(1)).alias("evaluated_rows")]
        aggregate_columns.extend(
            functions.coalesce(
                functions.sum(functions.when(failed_condition, 1).otherwise(0)),
                functions.lit(0),
            ).cast("long").alias(failure_alias)
            for failed_condition, failure_alias in zip(failed_conditions, failure_aliases, strict=True)
        )
        aggregate_columns.extend(
            (
                functions.coalesce(
                    functions.sum(functions.when(valid_condition, 1).otherwise(0)),
                    functions.lit(0),
                ).alias("accepted_rows"),
                functions.coalesce(
                    functions.sum(functions.when(quarantine_condition, 1).otherwise(0)),
                    functions.lit(0),
                ).alias("quarantined_rows"),
            )
        )
        counts = dataframe.agg(*aggregate_columns).first().asDict()
        rule_results = tuple(
            QualityRuleResult(
                rule_id=rule.rule_id,
                status="PASSED" if counts[failure_alias] == 0 else "FAILED",
                passed_rows=counts["evaluated_rows"] - counts[failure_alias],
                failed_rows=counts[failure_alias],
                evidence_ref=rule.evidence_ref,
                severity=rule.severity,
            )
            for rule, failure_alias in zip(self._rules, failure_aliases, strict=True)
        )
        failed_required_rule = any(
            counts[failure_alias] > 0 and rule.failure_action is QualityFailureAction.FAIL
            for rule, failure_alias in zip(self._rules, failure_aliases, strict=True)
        )
        has_non_blocking_failure = any(
            counts[failure_alias] > 0 and rule.failure_action is not QualityFailureAction.FAIL
            for rule, failure_alias in zip(self._rules, failure_aliases, strict=True)
        )
        return QualityEvaluation(
            valid_dataframe=dataframe.where(valid_condition),
            quarantine_dataframe=(
                dataframe.where(quarantine_condition)
                if any(rule.failure_action is QualityFailureAction.QUARANTINE for rule in self._rules)
                else None
            ),
            result=QualityRunResult(
                run_id=run_id,
                overall_status=(
                    DataQualityStatus.FAILED
                    if failed_required_rule
                    else DataQualityStatus.WARNING
                    if has_non_blocking_failure
                    else DataQualityStatus.PASSED
                ),
                rule_results=rule_results,
                evaluated_rows=counts["evaluated_rows"],
                accepted_rows=counts["accepted_rows"],
                quarantined_rows=counts["quarantined_rows"],
            ),
        )


__all__ = [
    "DataQualityEvaluator",
    "QualityEvaluation",
    "QualityFailureAction",
    "SparkSqlDataQualityEvaluator",
    "SparkSqlQualityRule",
]