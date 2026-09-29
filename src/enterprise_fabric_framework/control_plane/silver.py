"""Fabric SQL control-plane manager for frozen Bronze-to-Silver runs."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum

from .base import ControlPlaneManager, _optional_text, _required_text


class SilverCompletionState(StrEnum):
    """Completion states accepted by the Silver manifest contract."""

    PENDING = "PENDING"
    COMMITTED = "COMMITTED"
    COMPLETE = "COMPLETE"


@dataclass(frozen=True, slots=True)
class FrozenSilverPolicy:
    """Typed Silver mutation and schema policy selected for one frozen consumer run."""

    policy_schema_version: int
    schema_ref: str
    schema_columns: tuple[Mapping[str, object], ...]
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


@dataclass(frozen=True, slots=True)
class FrozenSilverConsumerPlan:
    """The consumer-specific projection of one frozen Silver run."""

    silver_run_id: str
    bronze_to_silver_config_id: str
    source_to_bronze_config_id: str
    bronze_relation_ref: str
    silver_relation_ref: str
    checkpoint_ref: str
    query_identity: str
    load_strategy: str
    policy: FrozenSilverPolicy


@dataclass(frozen=True, slots=True)
class SilverMicroBatch:
    """Bounded target-mutation evidence for one idempotent Silver micro-batch."""

    query_identity: str
    batch_id: int
    completion_state: SilverCompletionState
    silver_run_id: str | None = None
    input_rows: int | None = None
    accepted_rows: int | None = None
    quarantined_rows: int | None = None
    inserted_rows: int | None = None
    updated_rows: int | None = None
    deleted_rows: int | None = None
    target_commit_ref: str | None = None
    reconciliation_ref: str | None = None

    def __post_init__(self) -> None:
        if not self.query_identity.strip():
            raise ValueError("query_identity must not be empty")
        if self.batch_id < 0:
            raise ValueError("batch_id must be non-negative")
        for field_name in (
            "input_rows",
            "accepted_rows",
            "quarantined_rows",
            "inserted_rows",
            "updated_rows",
            "deleted_rows",
        ):
            value = getattr(self, field_name)
            if value is not None and value < 0:
                raise ValueError(f"{field_name} must be non-negative")


class SilverControlPlaneManager(ControlPlaneManager[FrozenSilverConsumerPlan]):
    """Moves frozen Silver settings into consumers and bounded micro-batch evidence to SQL."""

    _LOAD_PLAN = """
EXEC control.usp_get_silver_consumer_plan @silver_run_id = :silver_run_id
"""

    _RECORD_MANIFEST = """
EXEC control.usp_record_silver_manifest
    @query_identity = :query_identity,
    @batch_id = :batch_id,
    @silver_run_id = :silver_run_id,
    @input_rows = :input_rows,
    @accepted_rows = :accepted_rows,
    @quarantined_rows = :quarantined_rows,
    @inserted_rows = :inserted_rows,
    @updated_rows = :updated_rows,
    @deleted_rows = :deleted_rows,
    @target_commit_ref = :target_commit_ref,
    @reconciliation_ref = :reconciliation_ref,
    @completion_state = :completion_state
"""

    def load_plan(self, run_id: str) -> FrozenSilverConsumerPlan:
        """Load the immutable consumer configuration for one planned Silver run."""

        if not run_id.strip():
            raise ValueError("silver_run_id must not be empty")
        row = self._fetch_one(self._LOAD_PLAN, {"silver_run_id": run_id})
        if row is None:
            raise LookupError(f"no frozen Silver consumer plan exists for {run_id!r}")
        return _plan_from_row(row)

    def load_silver_consumer_plan(self, silver_run_id: str) -> FrozenSilverConsumerPlan:
        """Load the immutable consumer configuration for one planned Silver run."""

        return self.load_plan(silver_run_id)

    def record_silver_manifest(self, micro_batch: SilverMicroBatch) -> None:
        """Record bounded evidence for one Silver micro-batch without sending business rows to SQL."""

        self._execute(
            self._RECORD_MANIFEST,
            {
                "query_identity": micro_batch.query_identity,
                "batch_id": micro_batch.batch_id,
                "silver_run_id": micro_batch.silver_run_id,
                "input_rows": micro_batch.input_rows,
                "accepted_rows": micro_batch.accepted_rows,
                "quarantined_rows": micro_batch.quarantined_rows,
                "inserted_rows": micro_batch.inserted_rows,
                "updated_rows": micro_batch.updated_rows,
                "deleted_rows": micro_batch.deleted_rows,
                "target_commit_ref": micro_batch.target_commit_ref,
                "reconciliation_ref": micro_batch.reconciliation_ref,
                "completion_state": micro_batch.completion_state.value,
            },
        )


def _plan_from_row(row: Mapping[str, object]) -> FrozenSilverConsumerPlan:
    return FrozenSilverConsumerPlan(
        silver_run_id=_required_text(row, "silver_run_id"),
        bronze_to_silver_config_id=_required_text(row, "bronze_to_silver_config_id"),
        source_to_bronze_config_id=_required_text(row, "source_to_bronze_config_id"),
        bronze_relation_ref=_required_text(row, "bronze_relation_ref"),
        silver_relation_ref=_required_text(row, "silver_relation_ref"),
        checkpoint_ref=_required_text(row, "checkpoint_ref"),
        query_identity=_required_text(row, "query_identity"),
        load_strategy=_required_text(row, "load_strategy"),
        policy=FrozenSilverPolicy(
            policy_schema_version=_required_int(row, "policy_schema_version"),
            schema_ref=_required_text(row, "schema_ref"),
            schema_columns=_mapping_tuple(row, "schema_columns"),
            schema_mode=_required_text(row, "schema_mode"),
            entity_key_columns=_string_tuple(row, "entity_key_columns"),
            event_identity_columns=_string_tuple(row, "event_identity_columns"),
            ordering_columns=_string_tuple(row, "ordering_columns"),
            content_hash_ref=_optional_text(row, "content_hash_ref"),
            content_columns=_string_tuple(row, "content_columns"),
            source_operation_column=_optional_text(row, "source_operation_column"),
            delete_operation_values=_value_tuple(row, "delete_operation_values"),
            delete_marker_column=_optional_text(row, "delete_marker_column"),
            delete_marker_values=_value_tuple(row, "delete_marker_values"),
            delete_action=_optional_text(row, "delete_action") or "IGNORE",
            effective_time_column=_optional_text(row, "effective_time_column"),
            tracked_columns=_string_tuple(row, "tracked_columns"),
            late_arrival_policy=_optional_text(row, "late_arrival_policy"),
            correction_window_seconds=_optional_int(row, "correction_window_seconds"),
            snapshot_selection_ref=_optional_text(row, "snapshot_selection_ref"),
            snapshot_diff_apply_ref=_optional_text(row, "snapshot_diff_apply_ref"),
            replace_publication_ref=_optional_text(row, "replace_publication_ref"),
        ),
    )


def _required_int(row: Mapping[str, object], field_name: str) -> int:
    value = _optional_int(row, field_name)
    if value is None:
        raise ValueError(f"control-plane Silver consumer plan requires {field_name}")
    return value


def _optional_int(row: Mapping[str, object], field_name: str) -> int | None:
    value = row.get(field_name)
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"control-plane Silver consumer plan requires an integer {field_name}")
    try:
        return int(value)
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"control-plane Silver consumer plan requires an integer {field_name}"
        ) from error


def _string_tuple(row: Mapping[str, object], field_name: str) -> tuple[str, ...]:
    values = _value_tuple(row, field_name)
    if not all(isinstance(value, str) and value.strip() for value in values):
        raise ValueError(f"control-plane Silver consumer plan requires text values for {field_name}")
    return tuple(value.strip() for value in values)


def _mapping_tuple(row: Mapping[str, object], field_name: str) -> tuple[Mapping[str, object], ...]:
    values = _value_tuple(row, field_name)
    if not all(isinstance(value, Mapping) for value in values):
        raise ValueError(f"control-plane Silver consumer plan requires object values for {field_name}")
    return tuple(dict(value) for value in values if isinstance(value, Mapping))


def _value_tuple(row: Mapping[str, object], field_name: str) -> tuple[object, ...]:
    value = row.get(field_name)
    if value is None:
        return ()
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as error:
            raise ValueError(
                f"control-plane Silver consumer plan requires a JSON array for {field_name}"
            ) from error
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"control-plane Silver consumer plan requires an array for {field_name}")
    return tuple(value)


__all__ = [
    "FrozenSilverConsumerPlan",
    "FrozenSilverPolicy",
    "SilverCompletionState",
    "SilverControlPlaneManager",
    "SilverMicroBatch",
]