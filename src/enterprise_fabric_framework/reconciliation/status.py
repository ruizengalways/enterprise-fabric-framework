"""Reconciliation severity and aggregate status contracts."""

from __future__ import annotations

from enum import StrEnum


class ReconciliationSeverity(StrEnum):
    """Operational impact when one reconciliation rule does not pass."""

    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class ReconciliationStatus(StrEnum):
    """Aggregate status produced by one reconciliation evaluation."""

    PASSED = "PASSED"
    WARNING = "WARNING"
    FAILED = "FAILED"


__all__ = ["ReconciliationSeverity", "ReconciliationStatus"]