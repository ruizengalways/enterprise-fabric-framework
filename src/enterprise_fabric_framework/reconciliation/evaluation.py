"""Reusable bounded-evidence reconciliation contracts for Spark/Delta deliveries."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from enum import StrEnum
import json

from .status import ReconciliationSeverity, ReconciliationStatus


@dataclass(frozen=True, slots=True)
class ReconciliationEvidence:
    """Aggregate facts needed to reconcile one input-to-output delivery.

    Counts must be computed by the Spark or Delta capability that owns the data. This contract
    carries only bounded evidence, so it applies equally to Source-to-Bronze and Bronze-to-Silver.
    """

    input_rows: int | None
    output_rows: int | None
    rejected_rows: int | None = 0
    input_ref: str | None = None
    output_ref: str | None = None

    def __post_init__(self) -> None:
        for field_name in ("input_rows", "output_rows", "rejected_rows"):
            value = getattr(self, field_name)
            if value is not None and value < 0:
                raise ValueError(f"{field_name} must be non-negative")


@dataclass(frozen=True, slots=True)
class ReconciliationRuleResult:
    """Bounded outcome for one reconciliation rule."""

    rule_id: str
    passed: bool
    expected_rows: int | None = None
    actual_rows: int | None = None
    detail: str | None = None
    severity: ReconciliationSeverity = ReconciliationSeverity.CRITICAL


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    """Pipeline-facing reconciliation outcome containing no business rows."""

    run_id: str
    passed: bool
    rule_results: tuple[ReconciliationRuleResult, ...]
    evidence_ref: str | None = None
    status: ReconciliationStatus = ReconciliationStatus.PASSED

    def __post_init__(self) -> None:
        try:
            object.__setattr__(self, "status", ReconciliationStatus(self.status))
        except ValueError as error:
            raise ValueError("status must be a supported evaluation status") from error

    def to_json(self) -> str:
        """Serialize bounded evidence for a pipeline activity output or evidence store."""

        return json.dumps(asdict(self), separators=(",", ":"), sort_keys=True)

    @property
    def has_warnings(self) -> bool:
        """Return whether non-blocking reconciliation rules failed."""

        return self.status is ReconciliationStatus.WARNING

    @property
    def has_critical_failure(self) -> bool:
        """Return whether the reconciliation result blocks this table delivery."""

        return self.status is ReconciliationStatus.FAILED


class ReconciliationFailureAction(StrEnum):
    """Configured delivery treatment for a failed reconciliation rule."""

    REPORT = "REPORT"
    FAIL = "FAIL"


class ReconciliationRule(ABC):
    """Extension point for one bounded-evidence reconciliation check."""

    def __init__(
        self,
        rule_id: str,
        *,
        failure_action: ReconciliationFailureAction = ReconciliationFailureAction.FAIL,
        severity: ReconciliationSeverity = ReconciliationSeverity.CRITICAL,
    ) -> None:
        if not rule_id.strip():
            raise ValueError("rule_id must not be empty")
        if (
            failure_action is ReconciliationFailureAction.FAIL
            and severity is not ReconciliationSeverity.CRITICAL
        ):
            raise ValueError("a FAIL reconciliation rule must have CRITICAL severity")
        self._rule_id = rule_id
        self._failure_action = failure_action
        self._severity = severity

    @property
    def rule_id(self) -> str:
        """Return the rule's stable ID within its frozen layer plan."""

        return self._rule_id

    @property
    def failure_action(self) -> ReconciliationFailureAction:
        """Return how this rule affects the table-delivery outcome."""

        return self._failure_action

    @property
    def severity(self) -> ReconciliationSeverity:
        """Return the operational severity recorded when this rule fails."""

        return self._severity

    @abstractmethod
    def evaluate(self, evidence: ReconciliationEvidence) -> ReconciliationRuleResult:
        """Evaluate one bounded-evidence rule without reading business rows to Python."""

        raise NotImplementedError


class ReconciliationEvaluator(ABC):
    """Extension point for a delivery's reconciliation policy.

    A custom implementation can compare source and target aggregates, checksums, or domain
    invariants. It must calculate any business-data aggregates with Spark/Delta and return only
    the bounded result from this method.
    """

    @abstractmethod
    def evaluate(
        self,
        evidence: ReconciliationEvidence,
        *,
        run_id: str,
    ) -> ReconciliationResult:
        """Evaluate one delivery and return bounded reconciliation evidence."""

        raise NotImplementedError


class CompositeReconciliationEvaluator(ReconciliationEvaluator):
    """Evaluate every declared reconciliation rule for one table delivery."""

    def __init__(self, rules: Sequence[ReconciliationRule]) -> None:
        self._rules = tuple(rules)
        rule_ids = [rule.rule_id for rule in self._rules]
        if len(set(rule_ids)) != len(rule_ids):
            raise ValueError("reconciliation rule IDs must be unique")

    def evaluate(
        self,
        evidence: ReconciliationEvidence,
        *,
        run_id: str,
    ) -> ReconciliationResult:
        """Evaluate every rule so audit evidence is complete even when one rule fails."""

        rule_results = tuple(rule.evaluate(evidence) for rule in self._rules)
        critical_failure = any(
            not result.passed and rule.failure_action is ReconciliationFailureAction.FAIL
            for rule, result in zip(self._rules, rule_results, strict=True)
        )
        non_blocking_failure = any(not result.passed for result in rule_results) and not critical_failure
        return ReconciliationResult(
            run_id=run_id,
            passed=not critical_failure,
            rule_results=rule_results,
            status=(
                ReconciliationStatus.FAILED
                if critical_failure
                else ReconciliationStatus.WARNING
                if non_blocking_failure
                else ReconciliationStatus.PASSED
            ),
        )


class RowCountReconciliationRule(ReconciliationRule):
    """Verify the accounting invariant ``input = output + rejected`` for one delivery."""

    def evaluate(self, evidence: ReconciliationEvidence) -> ReconciliationRuleResult:
        """Evaluate row-count evidence without reading business data into Python."""

        if evidence.input_rows is None or evidence.output_rows is None:
            return ReconciliationRuleResult(
                rule_id=self._rule_id,
                passed=False,
                expected_rows=evidence.input_rows,
                actual_rows=None,
                detail="input_rows and output_rows are required for row-count reconciliation",
                severity=self._severity,
            )
        actual_rows = evidence.output_rows + (evidence.rejected_rows or 0)
        return ReconciliationRuleResult(
            rule_id=self._rule_id,
            passed=evidence.input_rows == actual_rows,
            expected_rows=evidence.input_rows,
            actual_rows=actual_rows,
            detail=None if evidence.input_rows == actual_rows else "row-count balance mismatch",
            severity=self._severity,
        )


class RowCountReconciliationEvaluator(CompositeReconciliationEvaluator):
    """Compatibility evaluator containing one configurable row-count rule."""

    def __init__(
        self,
        rule_id: str = "row-count-balance",
        *,
        failure_action: ReconciliationFailureAction = ReconciliationFailureAction.FAIL,
        severity: ReconciliationSeverity = ReconciliationSeverity.CRITICAL,
    ) -> None:
        super().__init__(
            (
                RowCountReconciliationRule(
                    rule_id,
                    failure_action=failure_action,
                    severity=severity,
                ),
            )
        )


__all__ = [
    "CompositeReconciliationEvaluator",
    "ReconciliationEvaluator",
    "ReconciliationEvidence",
    "ReconciliationFailureAction",
    "ReconciliationResult",
    "ReconciliationRule",
    "ReconciliationRuleResult",
    "ReconciliationSeverity",
    "ReconciliationStatus",
    "RowCountReconciliationEvaluator",
    "RowCountReconciliationRule",
]
