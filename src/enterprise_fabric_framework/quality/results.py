"""Bounded, JSON-ready data-quality evidence returned by Spark jobs."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json

from .status import DataQualitySeverity, DataQualityStatus


@dataclass(frozen=True, slots=True)
class QualityRuleResult:
    """Aggregate outcome for one DQ rule; it never contains business rows."""

    rule_id: str
    status: str
    passed_rows: int
    failed_rows: int
    evidence_ref: str | None = None
    severity: DataQualitySeverity = DataQualitySeverity.CRITICAL


@dataclass(frozen=True, slots=True)
class QualityViolationReference:
    """Bounded reference to one violation retained outside the pipeline payload."""

    rule_id: str
    violation_id: str
    violation_code: str
    record_identity_ref: str | None = None
    evidence_ref: str | None = None


@dataclass(frozen=True, slots=True)
class QualityRunResult:
    """Pipeline-facing result returned after DQ evaluation for one run or batch."""

    run_id: str
    overall_status: DataQualityStatus
    rule_results: tuple[QualityRuleResult, ...]
    violations: tuple[QualityViolationReference, ...] = ()
    batch_id: int | None = None
    evaluated_rows: int | None = None
    accepted_rows: int | None = None
    quarantined_rows: int | None = None

    def __post_init__(self) -> None:
        try:
            object.__setattr__(self, "overall_status", DataQualityStatus(self.overall_status))
        except ValueError as error:
            raise ValueError("overall_status must be a supported evaluation status") from error
        for field_name in ("evaluated_rows", "accepted_rows", "quarantined_rows"):
            value = getattr(self, field_name)
            if value is not None and value < 0:
                raise ValueError(f"{field_name} must be non-negative")

    def to_json(self) -> str:
        """Serialize bounded evidence for a Fabric Pipeline activity output."""

        return json.dumps(asdict(self), separators=(",", ":"), sort_keys=True)

    @property
    def has_critical_failure(self) -> bool:
        """Return whether the evaluation blocks this table delivery."""

        return self.overall_status is DataQualityStatus.FAILED

    @property
    def has_warnings(self) -> bool:
        """Return whether the evaluation completed with non-blocking rule failures."""

        return self.overall_status is DataQualityStatus.WARNING


__all__ = [
    "DataQualitySeverity",
    "DataQualityStatus",
    "QualityRuleResult",
    "QualityRunResult",
    "QualityViolationReference",
]