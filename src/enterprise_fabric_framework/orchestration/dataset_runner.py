"""Orchestration facade for the single Spark dataset runtime.

This module will coordinate control-plane state around the public Spark runtime. It must not
implement strategy-specific data transformations.
"""

from __future__ import annotations

from typing import Protocol

from enterprise_fabric_framework.contracts import RunResult, SparkRunRequest


class DatasetRuntime(Protocol):
    """Public runtime dependency consumed by the orchestration facade."""

    def run(self, request: SparkRunRequest) -> RunResult:
        """Run one frozen Bronze or Silver dataset plan."""

        ...


class DatasetRunner:
    """Stable orchestration entry point used by Fabric launchers and certification."""

    def __init__(self, runtime: DatasetRuntime) -> None:
        self._runtime = runtime

    def run(self, request: SparkRunRequest) -> RunResult:
        """Coordinate one opaque run identity through the public runtime."""

        raise NotImplementedError


def run_dataset(request: SparkRunRequest, runner: DatasetRunner) -> RunResult:
    """Function-shaped public entry point for a thin launcher."""

    raise NotImplementedError


__all__ = ["DatasetRunner", "DatasetRuntime", "run_dataset"]
