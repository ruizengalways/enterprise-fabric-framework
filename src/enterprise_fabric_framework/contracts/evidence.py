"""Bounded execution and rule-result values.

Detailed row-level failures belong in Delta evidence relations.  These contracts carry only
bounded counts, identities, status and references that a control-plane adapter can persist.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Mapping

from .control_plane import RuleKind

EvidenceStatus = Literal["PENDING", "PASSED", "FAILED", "COMPLETE"]


@dataclass(frozen=True, slots=True)
class RuleResult:
    owner_config_id: str
    run_id: str
    rule_id: str
    rule_kind: RuleKind
    status: EvidenceStatus
    batch_id: int | None = None
    passed_rows: int | None = None
    failed_rows: int | None = None
    metrics: Mapping[str, int | float | str | None] = field(default_factory=dict)
    evidence_ref: str | None = None


@dataclass(frozen=True, slots=True)
class BronzeManifestEvidence:
    bronze_run_id: str
    source_to_bronze_config_id: str
    delivery_id: str
    bronze_relation_ref: str
    completion_state: EvidenceStatus
    delta_version: int | None = None
    source_boundary_ref: str | None = None
    selected_rows: int | None = None
    published_rows: int | None = None
    rejected_rows: int | None = None
    evidence_ref: str | None = None


@dataclass(frozen=True, slots=True)
class SilverBatchEvidence:
    silver_run_id: str
    query_identity: str
    batch_id: int
    completion_state: EvidenceStatus
    input_rows: int | None = None
    accepted_rows: int | None = None
    quarantined_rows: int | None = None
    inserted_rows: int | None = None
    updated_rows: int | None = None
    deleted_rows: int | None = None
    target_commit_ref: str | None = None
    reconciliation_ref: str | None = None


@dataclass(frozen=True, slots=True)
class RunOutcome:
    run_id: str
    status: Literal["SUCCEEDED", "FAILED", "CANCELLED"]
    error_code: str | None = None
    evidence_refs: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class AuditEvent:
    event_id: str
    event_type: str
    run_id: str
    actor_id: str
    occurred_at: str
    attempt_id: str | None = None
    query_identity: str | None = None
    batch_id: int | None = None
    silver_run_id: str | None = None
    owner_id: str | None = None
    fencing_token: str | None = None
    target_commit_ref: str | None = None
    error_code: str | None = None
    evidence_ref: str | None = None


@dataclass(frozen=True, slots=True)
class EvidenceReceipt:
    evidence_ref: str
    recorded_at: str
