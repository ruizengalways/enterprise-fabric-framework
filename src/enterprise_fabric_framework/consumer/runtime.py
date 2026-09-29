"""Public runtime for Bronze-to-Silver consumer invocations."""

from __future__ import annotations

from dataclasses import dataclass

from enterprise_fabric_framework.control_plane import ControlPlanePort
from enterprise_fabric_framework.contracts import RunResult, SilverRunRequest
from enterprise_fabric_framework.producer.planning import CapabilityRegistry


@dataclass(frozen=True, slots=True)
class ConsumerRuntimeDependencies:
    control_plane: ControlPlanePort
    registry: CapabilityRegistry


class ConsumerRuntime:
    """Runs one frozen consumer query through Structured Streaming."""

    def __init__(self, dependencies: ConsumerRuntimeDependencies) -> None:
        self._dependencies = dependencies

    def run(self, request: SilverRunRequest) -> RunResult:
        """Execute one frozen consumer request."""

        raise NotImplementedError


__all__ = ["ConsumerRuntime", "ConsumerRuntimeDependencies"]
