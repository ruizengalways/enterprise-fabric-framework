"""Default SQL control-plane adapter seam.

This is intentionally only the shape of the default implementation. A company-owned adapter can
replace it while preserving the same ``ControlPlanePort`` semantics and contract tests.
"""

from __future__ import annotations

from typing import Any

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
class SqlControlPlaneAdapter:
    """Placeholder for the framework-provided SQL implementation."""

    def __init__(self, connection_factory: Any) -> None:
        self._connection_factory = connection_factory

    def load_bronze_plan(self, bronze_run_id: str) -> FrozenBronzePlan:
        raise NotImplementedError

    def load_silver_plan(self, silver_run_id: str) -> FrozenSilverPlan:
        raise NotImplementedError

    def acquire_lease(self, request: LeaseRequest) -> Lease:
        raise NotImplementedError

    def renew_lease(self, lease: Lease) -> Lease:
        raise NotImplementedError

    def release_lease(self, lease: Lease) -> None:
        raise NotImplementedError

    def record_bronze_manifest(
        self,
        evidence: BronzeManifestEvidence,
    ) -> EvidenceReceipt:
        raise NotImplementedError

    def record_rule_result(self, result: RuleResult) -> EvidenceReceipt:
        raise NotImplementedError

    def record_silver_batch(
        self,
        evidence: SilverBatchEvidence,
    ) -> EvidenceReceipt:
        raise NotImplementedError

    def record_run_outcome(self, outcome: RunOutcome) -> EvidenceReceipt:
        raise NotImplementedError

    def record_audit_event(self, event: AuditEvent) -> EvidenceReceipt:
        raise NotImplementedError


__all__ = ["SqlControlPlaneAdapter"]
