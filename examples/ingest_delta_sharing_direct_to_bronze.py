"""Append one directly configured Delta Sharing source selection to a Bronze Delta table.

Set ``DELTA_SHARING_PROFILE``, ``DELTA_SHARING_TABLE``, ``BRONZE_TABLE``, and
``CAPTURE_MODE`` before running in a Spark environment. Set ``SOURCE_VERSION`` for a FULL run,
or ``WATERMARK_COLUMN``, ``LOWER_BOUND``, and ``UPPER_BOUND`` for a WATERMARK run.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from enterprise_fabric_framework.connector import DeltaSharingConnector
from enterprise_fabric_framework.producer.readers import (
    DeltaSharingReader,
    SourceCaptureMode,
    SourceReadEvidence,
    SourceReadPlan,
    SourceReadRequest,
)

SparkSession = Any


@dataclass(frozen=True, slots=True)
class IngestionSettings:
    """Runtime settings for one simple Delta Sharing Source-to-Bronze delivery."""

    profile: str
    source_object: str
    bronze_table: str
    capture_mode: SourceCaptureMode
    watermark_column: str | None = None
    source_version: str | int | None = None
    lower_bound: object | None = None
    upper_bound: object | None = None


def settings_from_environment(
    environment: Mapping[str, str] = os.environ,
) -> IngestionSettings:
    """Read one source selection and target without querying a control plane."""

    capture_mode = _capture_mode(_required(environment, "CAPTURE_MODE"))
    settings = IngestionSettings(
        profile=_required(environment, "DELTA_SHARING_PROFILE"),
        source_object=_required(environment, "DELTA_SHARING_TABLE"),
        bronze_table=_required(environment, "BRONZE_TABLE"),
        capture_mode=capture_mode,
        watermark_column=environment.get("WATERMARK_COLUMN"),
        source_version=environment.get("SOURCE_VERSION"),
        lower_bound=environment.get("LOWER_BOUND"),
        upper_bound=environment.get("UPPER_BOUND"),
    )
    if capture_mode is SourceCaptureMode.WATERMARK:
        _required(environment, "WATERMARK_COLUMN")
        _required(environment, "LOWER_BOUND")
        _required(environment, "UPPER_BOUND")
    return settings


def build_request(settings: IngestionSettings) -> SourceReadRequest:
    """Build a typed source-read request from the selected capture mode."""

    plan = SourceReadPlan(
        source_object=settings.source_object,
        capture_mode=settings.capture_mode,
        watermark_column=settings.watermark_column,
    )
    if settings.capture_mode is SourceCaptureMode.FULL:
        return SourceReadRequest.for_snapshot(plan, source_version=settings.source_version)
    if settings.capture_mode is SourceCaptureMode.WATERMARK:
        if settings.lower_bound is None:
            raise ValueError("LOWER_BOUND must be set")
        if settings.upper_bound is None:
            raise ValueError("UPPER_BOUND must be set")
        return SourceReadRequest.for_watermark(
            plan,
            lower_bound=settings.lower_bound,
            upper_bound=settings.upper_bound,
        )
    raise ValueError(f"unsupported capture mode: {settings.capture_mode!r}")


def ingest(spark: SparkSession, settings: IngestionSettings) -> SourceReadEvidence:
    """Read the configured source selection and append it to the configured Bronze table."""

    reader = DeltaSharingReader(DeltaSharingConnector(spark, credentials=settings.profile))
    read_result = reader.read(build_request(settings))
    read_result.dataframe.write.format("delta").mode("append").saveAsTable(settings.bronze_table)
    return read_result.evidence


def main() -> None:
    """Run the simple standalone Delta Sharing Bronze delivery."""

    from pyspark.sql import SparkSession

    evidence = ingest(SparkSession.builder.getOrCreate(), settings_from_environment())
    print(f"Bronze publication completed for {evidence.source_object}.")


def _required(environment: Mapping[str, str], variable_name: str) -> str:
    value = environment.get(variable_name)
    if value is None or not value.strip():
        raise ValueError(f"{variable_name} must be set")
    return value


def _capture_mode(value: str) -> SourceCaptureMode:
    try:
        return SourceCaptureMode(value.upper())
    except ValueError as error:
        raise ValueError(f"CAPTURE_MODE is unsupported: {value!r}") from error


if __name__ == "__main__":
    main()