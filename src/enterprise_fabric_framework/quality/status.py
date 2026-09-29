"""Data-quality severity and aggregate status contracts."""

from __future__ import annotations

from enum import StrEnum


class DataQualitySeverity(StrEnum):
    """Operational impact when one data-quality rule does not pass."""

    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class DataQualityStatus(StrEnum):
    """Aggregate status produced by one data-quality evaluation."""

    PASSED = "PASSED"
    WARNING = "WARNING"
    FAILED = "FAILED"


__all__ = ["DataQualitySeverity", "DataQualityStatus"]