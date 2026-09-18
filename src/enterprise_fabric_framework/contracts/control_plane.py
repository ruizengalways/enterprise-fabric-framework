"""Framework-neutral frozen control-plane values.

These values are the seam between a control-plane adapter and Spark execution.  They contain
configuration, policy and capability references, never business rows or SQL connection objects.
Concrete SQL/Lakehouse/HTTP storage remains outside these contracts.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Mapping

RuleKind = Literal["QUALITY", "RECONCILIATION"]
FailureAction = Literal["FAIL", "REPORT", "QUARANTINE"]
SourceExecutionMode = Literal["BOUNDED", "STRUCTURED_STREAMING"]


@dataclass(frozen=True, slots=True)
class FrozenSourceToBronzeConfig:
    config_id: str
    dataset_id: str
    contract_version: int
    execution_group_id: str
    bronze_relation_ref: str
    capture_mode: str
    bronze_representation: str
    source_reader_ref: str | None = None
    source_connection_ref: str | None = None
    checkpoint_ref: str | None = None
    source_execution_mode: SourceExecutionMode = "BOUNDED"
    is_enabled: bool = True


@dataclass(frozen=True, slots=True)
class FrozenBronzeToSilverConfig:
    config_id: str
    dataset_id: str
    contract_version: int
    execution_group_id: str
    source_to_bronze_config_id: str
    silver_relation_ref: str
    checkpoint_ref: str
    load_strategy: str
    is_enabled: bool = True


@dataclass(frozen=True, slots=True)
class FrozenBronzePolicy:
    source_to_bronze_config_id: str
    source_fidelity: str
    record_identity_columns: tuple[str, ...]
    ordering_columns: tuple[str, ...]
    schema_ref: str
    schema_mode: str
    content_hash_ref: str | None = None
    content_columns: tuple[str, ...] = ()
    source_boundary_ref: str | None = None
    bootstrap_ref: str | None = None
    watermark_column: str | None = None
    watermark_tie_breaker_columns: tuple[str, ...] = ()
    watermark_timezone: str | None = None
    watermark_lookback_seconds: int | None = None
    snapshot_identity_column: str | None = None
    snapshot_scope_columns: tuple[str, ...] = ()
    snapshot_completeness_ref: str | None = None
    schema_columns: tuple[Mapping[str, object], ...] = ()


@dataclass(frozen=True, slots=True)
class FrozenSilverPolicy:
    bronze_to_silver_config_id: str
    schema_ref: str
    schema_mode: str
    entity_key_columns: tuple[str, ...] = ()
    event_identity_columns: tuple[str, ...] = ()
    ordering_columns: tuple[str, ...] = ()
    content_hash_ref: str | None = None
    content_columns: tuple[str, ...] = ()
    source_operation_column: str | None = None
    delete_operation_values: tuple[object, ...] = ()
    delete_marker_column: str | None = None
    delete_marker_values: tuple[object, ...] = ()
    delete_action: str = "IGNORE"
    effective_time_column: str | None = None
    tracked_columns: tuple[str, ...] = ()
    late_arrival_policy: str | None = None
    correction_window_seconds: int | None = None
    snapshot_selection_ref: str | None = None
    snapshot_diff_apply_ref: str | None = None
    replace_publication_ref: str | None = None
    schema_columns: tuple[Mapping[str, object], ...] = ()


@dataclass(frozen=True, slots=True)
class RuleSpec:
    """Declarative rule owned by exactly one producer or consumer configuration."""

    config_id: str
    rule_id: str
    rule_kind: RuleKind
    rule_ref: str
    execution_phase: str
    rule_order: int
    column_names: tuple[str, ...] = ()
    reference_relation_ref: str | None = None
    reference_metric_ref: str | None = None
    comparison: str | None = None
    threshold: float | None = None
    failure_action: FailureAction = "FAIL"


@dataclass(frozen=True, slots=True)
class FrozenBronzePlan:
    config: FrozenSourceToBronzeConfig
    policy: FrozenBronzePolicy
    rules: tuple[RuleSpec, ...]


@dataclass(frozen=True, slots=True)
class FrozenSilverPlan:
    config: FrozenBronzeToSilverConfig
    producer_config: FrozenSourceToBronzeConfig
    producer_policy: FrozenBronzePolicy
    policy: FrozenSilverPolicy
    rules: tuple[RuleSpec, ...]


@dataclass(frozen=True, slots=True)
class LeaseRequest:
    resource_key: str
    owner_run_id: str
    owner_id: str
    ttl_seconds: int


@dataclass(frozen=True, slots=True)
class Lease:
    resource_key: str
    lease_id: str
    fencing_token: str
    expires_at: str
