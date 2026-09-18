"""Single production entry point for one Structured Streaming dataset invocation.

Implementation is intentionally deferred until the current request and evidence contracts are
implemented. Stable per-contract checkpoints persist across scheduled micro-batch query runs.
"""

from __future__ import annotations

from dataclasses import dataclass

from enterprise_fabric_framework.control_plane import ControlPlanePort
from enterprise_fabric_framework.contracts import (
    BronzeRunRequest,
    RunResult,
    SilverRunRequest,
    SparkRunRequest,
)
from enterprise_fabric_framework.spark.registry import CapabilityRegistry


@dataclass(frozen=True, slots=True)
class SparkRuntimeDependencies:
    control_plane: ControlPlanePort
    registry: CapabilityRegistry


class SparkDatasetRuntime:
    """Public Spark runtime composed with replaceable control-plane and capability ports."""

    def __init__(self, dependencies: SparkRuntimeDependencies) -> None:
        self._dependencies = dependencies

    def run(self, request: SparkRunRequest) -> RunResult:
        """Execute one frozen Bronze or Silver request and return bounded outcome evidence."""

        raise NotImplementedError

    def run_bronze(self, request: BronzeRunRequest) -> RunResult:
        """Execute the Source-to-Bronze branch of the runtime."""

        raise NotImplementedError

    def run_silver(self, request: SilverRunRequest) -> RunResult:
        """Execute the Bronze-to-Silver Structured Streaming branch."""

        raise NotImplementedError


__all__ = ["SparkDatasetRuntime", "SparkRuntimeDependencies"]
