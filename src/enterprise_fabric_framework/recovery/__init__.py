"""Governed checkpoint, lease, target-version and rebuild recovery coordination."""

from __future__ import annotations

from dataclasses import dataclass

from enterprise_fabric_framework.contracts import RunResult
from enterprise_fabric_framework.orchestration import DatasetRunner


@dataclass(frozen=True, slots=True)
class RebuildRequest:
    execution_request_id: str
    dataset_id: str
    contract_version: int
    source_selection_ref: str


@dataclass(frozen=True, slots=True)
class RebuildPlan:
    execution_request_id: str
    replay_relation_ref: str
    replay_checkpoint_ref: str
    target_relation_ref: str


def plan_rebuild(request: RebuildRequest) -> RebuildPlan:
    """Create an approved, immutable replay plan for a compatible Silver contract."""

    raise NotImplementedError


def run_rebuild(plan: RebuildPlan, runner: DatasetRunner) -> RunResult:
    """Run the governed rebuild through the same public dataset runtime."""

    raise NotImplementedError


__all__ = ["RebuildPlan", "RebuildRequest", "plan_rebuild", "run_rebuild"]
