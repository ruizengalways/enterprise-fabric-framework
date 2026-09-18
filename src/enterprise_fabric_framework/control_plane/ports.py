"""Stable control-plane port used by orchestration and runtime.

The default SQL implementation and a domain/company-owned implementation must satisfy this port.
Spark strategy code depends on these semantic operations, never on table names or SQL APIs.
"""

from __future__ import annotations

from typing import Protocol

from enterprise_fabric_framework.contracts import (
    AuditEvent,
    BronzeManifestEvidence,
    EvidenceReceipt,
    FrozenBronzePlan,
    FrozenSilverPlan,
    Lease,
    LeaseRequest,
    RuleResult,
    RunOutcome,
    SilverBatchEvidence,
)


class ControlPlanePort(Protocol):
    """Semantic read/write boundary for frozen plans and bounded runtime evidence."""

    def load_bronze_plan(self, bronze_run_id: str) -> FrozenBronzePlan:
        """Load one immutable Source-to-Bronze plan."""

        ...

    def load_silver_plan(self, silver_run_id: str) -> FrozenSilverPlan:
        """Load one immutable Bronze-to-Silver plan."""

        ...

    def acquire_lease(self, request: LeaseRequest) -> Lease:
        """Claim a query, target or provider resource with fencing."""

        ...

    def renew_lease(self, lease: Lease) -> Lease:
        """Renew a lease using its current fencing identity."""

        ...

    def release_lease(self, lease: Lease) -> None:
        """Release a lease after the owner has stopped mutating resources."""

        ...

    def record_bronze_manifest(
        self,
        evidence: BronzeManifestEvidence,
    ) -> EvidenceReceipt:
        """Persist bounded Source-to-Bronze publication evidence."""

        ...

    def record_rule_result(self, result: RuleResult) -> EvidenceReceipt:
        """Persist one idempotent rule outcome or its evidence reference."""

        ...

    def record_silver_batch(
        self,
        evidence: SilverBatchEvidence,
    ) -> EvidenceReceipt:
        """Persist one logical Silver micro-batch result."""

        ...

    def record_run_outcome(self, outcome: RunOutcome) -> EvidenceReceipt:
        """Persist a terminal Bronze or Silver run outcome."""

        ...

    def record_audit_event(self, event: AuditEvent) -> EvidenceReceipt:
        """Persist a bounded lifecycle or operator audit event."""

        ...


__all__ = ["ControlPlanePort"]
