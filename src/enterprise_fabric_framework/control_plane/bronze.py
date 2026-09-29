"""Fabric SQL control-plane manager for frozen Source-to-Bronze runs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum

from enterprise_fabric_framework.producer.readers import (
    SourceCaptureMode,
    SourceReadBoundary,
    SourceReadPlan,
    SourceReadRequest,
    SourceReadResult,
    SourceReader,
)

from .base import ControlPlaneManager, _optional_text, _required_text


class BronzeCompletionState(StrEnum):
    """Completion states accepted by the Bronze manifest contract."""

    PENDING = "PENDING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class FrozenBronzeReaderPlan:
    """The reader-specific projection of one frozen Bronze run."""

    bronze_run_id: str
    source_to_bronze_config_id: str
    bronze_relation_ref: str
    source_boundary_ref: str | None
    source_read_plan: SourceReadPlan

    def request_for(self, boundary: SourceReadBoundary) -> SourceReadRequest:
        """Combine the frozen source settings with a boundary proven for this run."""

        return SourceReadRequest(plan=self.source_read_plan, boundary=boundary)


@dataclass(frozen=True, slots=True)
class BronzePublication:
    """Bounded Bronze publication evidence supplied after data has been written."""

    delivery_id: str
    bronze_run_id: str
    source_to_bronze_config_id: str
    bronze_relation_ref: str
    completion_state: BronzeCompletionState
    source_boundary_ref: str | None = None
    snapshot_identity_ref: str | None = None
    delta_version: int | None = None
    published_rows: int | None = None
    rejected_rows: int | None = None
    evidence_ref: str | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "delivery_id",
            "bronze_run_id",
            "source_to_bronze_config_id",
            "bronze_relation_ref",
        ):
            if not getattr(self, field_name).strip():
                raise ValueError(f"{field_name} must not be empty")
        for field_name in ("delta_version", "published_rows", "rejected_rows"):
            value = getattr(self, field_name)
            if value is not None and value < 0:
                raise ValueError(f"{field_name} must be non-negative")


class BronzeControlPlaneManager(ControlPlaneManager[FrozenBronzeReaderPlan]):
    """Moves frozen Bronze settings into readers and bounded evidence back to SQL."""

    _LOAD_PLAN = """
EXEC control.usp_get_bronze_reader_plan @bronze_run_id = :bronze_run_id
"""

    _RECORD_MANIFEST = """
EXEC control.usp_record_bronze_manifest
    @delivery_id = :delivery_id,
    @bronze_run_id = :bronze_run_id,
    @source_to_bronze_config_id = :source_to_bronze_config_id,
    @bronze_relation_ref = :bronze_relation_ref,
    @completion_state = :completion_state,
    @source_boundary_ref = :source_boundary_ref,
    @snapshot_identity_ref = :snapshot_identity_ref,
    @delta_version = :delta_version,
    @selected_rows = :selected_rows,
    @published_rows = :published_rows,
    @rejected_rows = :rejected_rows,
    @evidence_ref = :evidence_ref
"""

    def load_plan(self, run_id: str) -> FrozenBronzeReaderPlan:
        """Load the immutable reader configuration for one planned Bronze run."""

        if not run_id.strip():
            raise ValueError("bronze_run_id must not be empty")
        row = self._fetch_one(self._LOAD_PLAN, {"bronze_run_id": run_id})
        if row is None:
            raise LookupError(f"no frozen Bronze reader plan exists for {run_id!r}")
        return _plan_from_row(row)

    def load_bronze_reader_plan(self, bronze_run_id: str) -> FrozenBronzeReaderPlan:
        """Load the immutable reader configuration for one planned Bronze run."""

        return self.load_plan(bronze_run_id)

    def read(
        self,
        bronze_run_id: str,
        reader: SourceReader,
        boundary: SourceReadBoundary,
    ) -> SourceReadResult:
        """Pass the selected frozen settings and proven boundary to one reader."""

        return reader.read(self.load_plan(bronze_run_id).request_for(boundary))

    def record_bronze_manifest(
        self,
        publication: BronzePublication,
        read_result: SourceReadResult,
    ) -> None:
        """Record bounded publication evidence after Bronze publication.

        This method never sends ``read_result.dataframe`` to SQL and never advances a source
        cursor. Cursor advancement is a separate fenced operation after complete publication.
        """

        self._execute(
            self._RECORD_MANIFEST,
            {
                "delivery_id": publication.delivery_id,
                "bronze_run_id": publication.bronze_run_id,
                "source_to_bronze_config_id": publication.source_to_bronze_config_id,
                "bronze_relation_ref": publication.bronze_relation_ref,
                "completion_state": publication.completion_state.value,
                "source_boundary_ref": publication.source_boundary_ref,
                "snapshot_identity_ref": publication.snapshot_identity_ref,
                "delta_version": publication.delta_version,
                "selected_rows": read_result.evidence.source_row_count,
                "published_rows": publication.published_rows,
                "rejected_rows": publication.rejected_rows,
                "evidence_ref": publication.evidence_ref,
            },
        )


def _plan_from_row(row: Mapping[str, object]) -> FrozenBronzeReaderPlan:
    try:
        capture_mode = SourceCaptureMode(_required_text(row, "capture_mode").upper())
    except ValueError as error:
        raise ValueError(f"unsupported control-plane capture_mode: {row.get('capture_mode')!r}") from error
    watermark_column = _optional_text(row, "watermark_column")
    return FrozenBronzeReaderPlan(
        bronze_run_id=_required_text(row, "bronze_run_id"),
        source_to_bronze_config_id=_required_text(row, "source_to_bronze_config_id"),
        bronze_relation_ref=_required_text(row, "bronze_relation_ref"),
        source_boundary_ref=_optional_text(row, "source_boundary_ref"),
        source_read_plan=SourceReadPlan(
            source_object=_required_text(row, "source_object_ref"),
            capture_mode=capture_mode,
            watermark_column=watermark_column,
        ),
    )


__all__ = [
    "BronzeCompletionState",
    "BronzeControlPlaneManager",
    "BronzePublication",
    "FrozenBronzeReaderPlan",
]