"""Fabric Pipeline, Notebook and Spark Job invocation adapter boundary.

The implementation will validate explicit bounded parameters and normalize them into the public
Spark run request. Bronze relations and stable query checkpoint paths come from frozen metadata.
It must not query ambient Pipeline state or create/update Fabric workspace items.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from enterprise_fabric_framework.contracts import RunResult, SparkRunRequest
from enterprise_fabric_framework.orchestration import DatasetRunner


@dataclass(frozen=True, slots=True)
class FabricInvocation:
    """Explicit parameters forwarded by a Fabric Pipeline to a thin launcher."""

    request_schema_version: int
    bronze_run_id: str | None = None
    silver_run_id: str | None = None
    execution_request_id: str | None = None


def normalize_invocation(raw_parameters: Mapping[str, str]) -> FabricInvocation:
    """Normalize explicit Pipeline/Job/Notebook parameters without reading ambient state."""

    raise NotImplementedError


def to_spark_request(invocation: FabricInvocation) -> SparkRunRequest:
    """Convert normalized Fabric parameters to the public Spark request contract."""

    raise NotImplementedError


def run_fabric_invocation(
    raw_parameters: Mapping[str, str],
    runner: DatasetRunner,
) -> RunResult:
    """Thin-launcher function boundary used by a domain Fabric Spark Job Definition."""

    raise NotImplementedError


__all__ = [
    "FabricInvocation",
    "normalize_invocation",
    "run_fabric_invocation",
    "to_spark_request",
]
