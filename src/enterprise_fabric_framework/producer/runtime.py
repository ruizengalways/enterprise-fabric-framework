"""Public runtime for Source-to-Bronze producer invocations."""

from __future__ import annotations

from dataclasses import dataclass

from enterprise_fabric_framework.control_plane import ControlPlanePort
from enterprise_fabric_framework.contracts import BronzeRunRequest, RunResult
from enterprise_fabric_framework.producer.pipeline import SourceToBronzePipeline
from enterprise_fabric_framework.producer.planning import CapabilityRegistry


@dataclass(frozen=True, slots=True)
class ProducerRuntimeDependencies:
    control_plane: ControlPlanePort
    registry: CapabilityRegistry
    pipeline: SourceToBronzePipeline


class ProducerRuntime:
    """Runs one frozen producer plan through a selected lifecycle implementation."""

    def __init__(self, dependencies: ProducerRuntimeDependencies) -> None:
        self._dependencies = dependencies

    def run(self, request: BronzeRunRequest) -> RunResult:
        """Execute one frozen producer request."""

        raise NotImplementedError


__all__ = ["ProducerRuntime", "ProducerRuntimeDependencies"]
