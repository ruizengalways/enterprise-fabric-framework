"""Delta Sharing Source-to-Bronze job settings and execution composition."""

from __future__ import annotations

import os
from abc import ABC
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from enterprise_fabric_framework.connector import FabricSqlDatabase
from enterprise_fabric_framework.control_plane import (
    BronzeCompletionState,
    BronzeControlPlaneManager,
    BronzePublication,
    ExecutionGroupControlPlaneManager,
    FrozenBronzeReaderPlan,
)
from enterprise_fabric_framework.orchestration import (
    ExecutionGroupJob,
    RuntimeConfiguredTableDeliveryExecutor,
    TableDeliveryAuditManager,
    TableDeliveryRuntimeFactory,
)
from enterprise_fabric_framework.producer.readers import (
    DeltaSharingReader,
    SnapshotReadBoundary,
    SourceCaptureMode,
    SourceReadBoundary,
    SourceReadEvidence,
    WatermarkReadBoundary,
)
from enterprise_fabric_framework.quality import DataQualityEvaluator, DataQualityStatus
from enterprise_fabric_framework.reconciliation import (
    ReconciliationEvaluator,
    ReconciliationEvidence,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class BronzeDeliverySettings:
    """Common environment-bound settings for one targeted Bronze table delivery."""

    control_plane_connection_url: str
    bronze_run_id: str
    delivery_id: str
    completion_state: BronzeCompletionState
    delta_version: int | None = None
    published_rows: int = 0
    rejected_rows: int | None = None
    evidence_ref: str | None = None
    snapshot_identity_ref: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class DeltaSharingBronzeDeliverySettings(BronzeDeliverySettings):
    """Bronze delivery settings specific to a Delta Sharing source connection."""

    delta_sharing_profile: str

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] = os.environ,
    ) -> DeltaSharingBronzeDeliverySettings:
        """Read the Delta Sharing delivery settings supplied by a targeted job invocation."""

        return cls(
            control_plane_connection_url=_required(environment, "CONTROL_PLANE_SQLALCHEMY_URL"),
            delta_sharing_profile=_required(environment, "DELTA_SHARING_PROFILE"),
            bronze_run_id=_required(environment, "BRONZE_RUN_ID"),
            delivery_id=_required(environment, "BRONZE_DELIVERY_ID"),
            completion_state=BronzeCompletionState(
                _required(environment, "BRONZE_COMPLETION_STATE").upper()
            ),
            delta_version=_optional_int(environment, "BRONZE_DELTA_VERSION"),
            published_rows=_required_int(environment, "BRONZE_PUBLISHED_ROWS"),
            rejected_rows=_optional_int(environment, "BRONZE_REJECTED_ROWS"),
            evidence_ref=environment.get("BRONZE_EVIDENCE_REF"),
            snapshot_identity_ref=environment.get("SNAPSHOT_IDENTITY_REF"),
        )


class DeltaSharingTableRuntimeFactory(TableDeliveryRuntimeFactory, ABC):
    """Customization base for resolving runtime components of Delta Sharing table deliveries.

    Subclasses resolve every frozen delivery's ingestion-method, DQ, and reconciliation references
    while using the supplied Spark session and Delta Sharing profile. A factory is invoked again
    for every table, so connector-specific state cannot leak between deliveries.
    """

    def __init__(self, spark: Any, delta_sharing_profile: str) -> None:
        if not delta_sharing_profile.strip():
            raise ValueError("delta_sharing_profile must not be empty")
        self._spark = spark
        self._delta_sharing_profile = delta_sharing_profile

    @property
    def spark(self) -> Any:
        """Return the Spark session bound to this Fabric job."""

        return self._spark

    @property
    def delta_sharing_profile(self) -> str:
        """Return the deployment-resolved Delta Sharing credentials profile."""

        return self._delta_sharing_profile


class DeltaSharingExecutionGroupJob(ExecutionGroupJob):
    """Execution-group job for table deliveries resolved by a Delta Sharing runtime factory."""

    def __init__(
        self,
        control_plane_connection_url: str,
        runtime_factory: DeltaSharingTableRuntimeFactory,
        *,
        audit_manager: TableDeliveryAuditManager | None = None,
    ) -> None:
        if not control_plane_connection_url.strip():
            raise ValueError("control_plane_connection_url must not be empty")
        self._audit_manager = audit_manager or ExecutionGroupControlPlaneManager(
            FabricSqlDatabase(control_plane_connection_url)
        )
        self._runtime_factory = runtime_factory

    def create_audit_manager(self) -> TableDeliveryAuditManager:
        """Return the manager that reads frozen deliveries and writes bounded audit evidence."""

        return self._audit_manager

    def create_table_executor(self) -> RuntimeConfiguredTableDeliveryExecutor:
        """Return the executor that constructs components independently for each table."""

        return RuntimeConfiguredTableDeliveryExecutor(self._runtime_factory)


def boundary_from_environment(
    plan: FrozenBronzeReaderPlan,
    environment: Mapping[str, str] = os.environ,
) -> SourceReadBoundary:
    """Supply the source boundary proven for a targeted single-table delivery."""

    if plan.source_read_plan.capture_mode is SourceCaptureMode.FULL:
        return SnapshotReadBoundary(source_version=environment.get("SOURCE_VERSION"))
    if plan.source_read_plan.capture_mode is SourceCaptureMode.WATERMARK:
        return WatermarkReadBoundary(
            lower_bound=_required(environment, "LOWER_BOUND"),
            upper_bound=_required(environment, "UPPER_BOUND"),
        )
    raise ValueError(f"unsupported capture mode: {plan.source_read_plan.capture_mode!r}")


def ingest_delta_sharing_delivery(
    manager: BronzeControlPlaneManager,
    reader: DeltaSharingReader,
    settings: BronzeDeliverySettings,
    environment: Mapping[str, str] = os.environ,
    *,
    quality_evaluator: DataQualityEvaluator | None = None,
    reconciliation_evaluator: ReconciliationEvaluator | None = None,
) -> SourceReadEvidence:
    """Read, evaluate, publish, and reconcile one frozen Delta Sharing table delivery."""

    plan = manager.load_bronze_reader_plan(settings.bronze_run_id)
    read_result = reader.read(plan.request_for(boundary_from_environment(plan, environment)))
    quality_evaluation = (
        quality_evaluator.evaluate(read_result.dataframe, run_id=settings.bronze_run_id)
        if quality_evaluator is not None
        else None
    )
    if (
        quality_evaluation is not None
        and quality_evaluation.result.overall_status is DataQualityStatus.FAILED
    ):
        raise RuntimeError("data-quality evaluation did not pass; Bronze publication was skipped")
    if quality_evaluation is not None and quality_evaluation.result.quarantined_rows:
        raise RuntimeError(
            "quarantined rows require a configured quarantine sink; Bronze publication was skipped"
        )

    dataframe_to_publish = (
        quality_evaluation.valid_dataframe
        if quality_evaluation is not None
        else read_result.dataframe
    )
    dataframe_to_publish.write.format("delta").mode("append").saveAsTable(plan.bronze_relation_ref)
    source_rows = (
        quality_evaluation.result.evaluated_rows
        if quality_evaluation is not None
        else read_result.evidence.source_row_count
    )
    if reconciliation_evaluator is not None:
        reconciliation_result = reconciliation_evaluator.evaluate(
            ReconciliationEvidence(
                input_rows=source_rows,
                output_rows=settings.published_rows,
                rejected_rows=settings.rejected_rows,
                input_ref=plan.source_read_plan.source_object,
                output_ref=plan.bronze_relation_ref,
            ),
            run_id=settings.bronze_run_id,
        )
        if not reconciliation_result.passed:
            raise RuntimeError("reconciliation did not pass; Bronze delivery was not recorded")
    manager.record_bronze_manifest(
        BronzePublication(
            delivery_id=settings.delivery_id,
            bronze_run_id=plan.bronze_run_id,
            source_to_bronze_config_id=plan.source_to_bronze_config_id,
            bronze_relation_ref=plan.bronze_relation_ref,
            completion_state=settings.completion_state,
            source_boundary_ref=plan.source_boundary_ref,
            snapshot_identity_ref=settings.snapshot_identity_ref,
            delta_version=settings.delta_version,
            published_rows=settings.published_rows,
            rejected_rows=settings.rejected_rows,
            evidence_ref=settings.evidence_ref,
        ),
        read_result,
    )
    return read_result.evidence


def _required(environment: Mapping[str, str], variable_name: str) -> str:
    value = environment.get(variable_name)
    if value is None or not value.strip():
        raise ValueError(f"{variable_name} must be set")
    return value


def _optional_int(environment: Mapping[str, str], variable_name: str) -> int | None:
    value = environment.get(variable_name)
    return None if value is None or not value.strip() else int(value)


def _required_int(environment: Mapping[str, str], variable_name: str) -> int:
    value = _required(environment, variable_name)
    try:
        return int(value)
    except ValueError as error:
        raise ValueError(f"{variable_name} must be an integer") from error


__all__ = [
    "BronzeDeliverySettings",
    "DeltaSharingBronzeDeliverySettings",
    "DeltaSharingExecutionGroupJob",
    "DeltaSharingTableRuntimeFactory",
    "boundary_from_environment",
    "ingest_delta_sharing_delivery",
]