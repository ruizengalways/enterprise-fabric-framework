"""Fabric SQL control-plane adapter for frozen multi-table Bronze executions."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence

from enterprise_fabric_framework.orchestration import (
    TableDelivery,
    TableDeliveryAuditManager,
    TableDeliveryOutcome,
)
from enterprise_fabric_framework.quality import QualityRunResult
from enterprise_fabric_framework.reconciliation import ReconciliationResult

from .base import ControlPlaneManager, _required_text


class ExecutionGroupControlPlaneManager(
    ControlPlaneManager[tuple[TableDelivery, ...]],
    TableDeliveryAuditManager,
):
    """Load frozen table deliveries and persist their bounded rule and outcome evidence.

    Subclass this manager when a deployment uses a different database adapter or procedure
    naming convention. The parent pipeline invokes ``load_table_deliveries`` once; all fields in
    the returned deliveries come from the frozen plan rather than mutable desired-state metadata.
    """

    _LOAD_DELIVERIES = """
EXEC control.usp_get_execution_group_bronze_deliveries
    @pipeline_run_id = :pipeline_run_id
"""

    _RECORD_DQ_RESULT = """
EXEC control.usp_record_bronze_dq_result
    @bronze_run_id = :bronze_run_id,
    @rule_id = :rule_id,
    @status = :status,
    @severity = :severity,
    @passed_rows = :passed_rows,
    @failed_rows = :failed_rows,
    @evidence_ref = :evidence_ref
"""

    _RECORD_DQ_VIOLATION = """
EXEC control.usp_record_bronze_dq_violation
    @bronze_run_id = :bronze_run_id,
    @rule_id = :rule_id,
    @violation_id = :violation_id,
    @violation_code = :violation_code,
    @record_identity_ref = :record_identity_ref,
    @evidence_ref = :evidence_ref
"""

    _RECORD_RECON_RULE_RESULT = """
EXEC control.usp_record_bronze_recon_rule_result
    @bronze_run_id = :bronze_run_id,
    @rule_id = :rule_id,
    @status = :status,
    @severity = :severity,
    @expected_rows = :expected_rows,
    @actual_rows = :actual_rows,
    @detail = :detail
"""

    _RECORD_RECON_RESULT = """
EXEC control.usp_record_bronze_recon_result
    @bronze_run_id = :bronze_run_id,
    @status = :status,
    @evidence_ref = :evidence_ref
"""

    _RECORD_TABLE_OUTCOME = """
EXEC control.usp_record_bronze_run_outcome
    @bronze_run_id = :bronze_run_id,
    @status = :status,
    @error_code = :error_code,
    @evidence_ref = :evidence_ref
"""

    def load_plan(self, pipeline_run_id: str) -> tuple[TableDelivery, ...]:
        """Load the frozen table plans selected for one parent pipeline run."""

        return self.load_table_deliveries(pipeline_run_id)

    def load_table_deliveries(self, pipeline_run_id: str) -> tuple[TableDelivery, ...]:
        """Return all frozen table deliveries, including runtime capability references."""

        if not pipeline_run_id.strip():
            raise ValueError("pipeline_run_id must not be empty")
        rows = self._fetch_all(self._LOAD_DELIVERIES, {"pipeline_run_id": pipeline_run_id})
        return tuple(_table_delivery_from_row(pipeline_run_id, row) for row in rows)

    def record_quality_result(self, delivery: TableDelivery, result: QualityRunResult) -> None:
        """Persist each DQ rule and bounded violation reference for one table delivery."""

        for rule_result in result.rule_results:
            self._execute(
                self._RECORD_DQ_RESULT,
                {
                    "bronze_run_id": delivery.bronze_run_id,
                    "rule_id": rule_result.rule_id,
                    "status": rule_result.status,
                    "severity": rule_result.severity.value,
                    "passed_rows": rule_result.passed_rows,
                    "failed_rows": rule_result.failed_rows,
                    "evidence_ref": rule_result.evidence_ref,
                },
            )
        for violation in result.violations:
            self._execute(
                self._RECORD_DQ_VIOLATION,
                {
                    "bronze_run_id": delivery.bronze_run_id,
                    "rule_id": violation.rule_id,
                    "violation_id": violation.violation_id,
                    "violation_code": violation.violation_code,
                    "record_identity_ref": violation.record_identity_ref,
                    "evidence_ref": violation.evidence_ref,
                },
            )

    def record_reconciliation_result(
        self,
        delivery: TableDelivery,
        result: ReconciliationResult,
    ) -> None:
        """Persist each reconciliation rule followed by its aggregate table outcome."""

        for rule_result in result.rule_results:
            self._execute(
                self._RECORD_RECON_RULE_RESULT,
                {
                    "bronze_run_id": delivery.bronze_run_id,
                    "rule_id": rule_result.rule_id,
                    "status": "PASSED" if rule_result.passed else "FAILED",
                    "severity": rule_result.severity.value,
                    "expected_rows": rule_result.expected_rows,
                    "actual_rows": rule_result.actual_rows,
                    "detail": rule_result.detail,
                },
            )
        self._execute(
            self._RECORD_RECON_RESULT,
            {
                "bronze_run_id": delivery.bronze_run_id,
                "status": result.status.value,
                "evidence_ref": result.evidence_ref,
            },
        )

    def record_table_outcome(self, outcome: TableDeliveryOutcome) -> None:
        """Persist one table's final state after all available evidence is recorded."""

        self._execute(
            self._RECORD_TABLE_OUTCOME,
            {
                "bronze_run_id": outcome.delivery.bronze_run_id,
                "status": outcome.status.value,
                "error_code": outcome.error_code,
                "evidence_ref": (
                    outcome.evidence.evidence_ref if outcome.evidence is not None else None
                ),
            },
        )


def _table_delivery_from_row(
    pipeline_run_id: str,
    row: Mapping[str, object],
) -> TableDelivery:
    return TableDelivery(
        pipeline_run_id=pipeline_run_id,
        bronze_run_id=_required_text(row, "bronze_run_id"),
        source_to_bronze_config_id=_required_text(row, "source_to_bronze_config_id"),
        source_object=_required_text(row, "source_object_ref"),
        ingestion_method_ref=_required_text(row, "ingestion_method_ref"),
        data_quality_rule_refs=_reference_tuple(row, "data_quality_rule_refs"),
        reconciliation_rule_refs=_reference_tuple(row, "reconciliation_rule_refs"),
    )


def _reference_tuple(row: Mapping[str, object], field_name: str) -> tuple[str, ...]:
    value = row.get(field_name)
    if value is None:
        return ()
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as error:
            raise ValueError(f"{field_name} must be a JSON array") from error
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"{field_name} must be an array")
    if not all(isinstance(item, str) and item.strip() for item in value):
        raise ValueError(f"{field_name} must contain non-empty text references")
    return tuple(item.strip() for item in value if isinstance(item, str))


__all__ = ["ExecutionGroupControlPlaneManager"]