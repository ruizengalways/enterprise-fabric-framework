"""Execution-group planning boundary.

The planner freezes metadata and bindings into immutable Bronze/Silver plans. Fabric schedules and
workspace item definitions remain outside this package.
"""

from __future__ import annotations

from dataclasses import dataclass

from enterprise_fabric_framework.contracts import FrozenBronzePlan, FrozenSilverPlan


@dataclass(frozen=True, slots=True)
class PlanExecutionGroupRequest:
    execution_group_id: str
    environment: str
    fabric_pipeline_run_id: str


@dataclass(frozen=True, slots=True)
class PlannedExecutionGroup:
    bronze_plans: tuple[FrozenBronzePlan, ...]
    silver_plans: tuple[FrozenSilverPlan, ...]


def plan_execution_group(
    request: PlanExecutionGroupRequest,
) -> PlannedExecutionGroup:
    """Validate enabled metadata and return frozen producer/consumer plans."""

    raise NotImplementedError


__all__ = ["PlanExecutionGroupRequest", "PlannedExecutionGroup", "plan_execution_group"]
