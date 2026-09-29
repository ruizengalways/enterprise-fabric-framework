from __future__ import annotations

import unittest

from examples.ingest_delta_sharing_execution_group_to_bronze import ingest_execution_group
from enterprise_fabric_framework.orchestration import (
    ExecutionGroupStatus,
    TableDelivery,
    TableDeliveryAuditManager,
    TableDeliveryEvidence,
    TableDeliveryOutcome,
    TableIngestionMethod,
)
from enterprise_fabric_framework.producer.delta_sharing_job import (
    DeltaSharingBronzeDeliverySettings,
    DeltaSharingTableRuntimeFactory,
    ingest_delta_sharing_delivery,
)
from enterprise_fabric_framework.control_plane import (
    BronzeCompletionState,
    BronzePublication,
    FrozenBronzeReaderPlan,
)
from enterprise_fabric_framework.producer.readers import (
    SnapshotReadBoundary,
    SourceCaptureMode,
    SourceReadEvidence,
    SourceReadPlan,
    SourceReadRequest,
    SourceReadResult,
)
from enterprise_fabric_framework.quality import QualityEvaluation, QualityRunResult
from enterprise_fabric_framework.reconciliation import ReconciliationResult, ReconciliationRuleResult


class _DataFrameWriter:
    def __init__(self) -> None:
        self.table_name: str | None = None

    def format(self, _: str) -> _DataFrameWriter:
        return self

    def mode(self, _: str) -> _DataFrameWriter:
        return self

    def saveAsTable(self, table_name: str) -> None:
        self.table_name = table_name


class _DataFrame:
    def __init__(self) -> None:
        self.write = _DataFrameWriter()


class _Manager:
    def __init__(self, plan: FrozenBronzeReaderPlan) -> None:
        self.plan = plan
        self.requested_run_id: str | None = None
        self.publication: BronzePublication | None = None
        self.read_result: SourceReadResult | None = None

    def load_bronze_reader_plan(self, bronze_run_id: str) -> FrozenBronzeReaderPlan:
        self.requested_run_id = bronze_run_id
        return self.plan

    def record_bronze_manifest(
        self,
        publication: BronzePublication,
        read_result: SourceReadResult,
    ) -> None:
        self.publication = publication
        self.read_result = read_result


class _Reader:
    def __init__(self, result: SourceReadResult) -> None:
        self.result = result
        self.request: SourceReadRequest | None = None

    def read(self, request: SourceReadRequest) -> SourceReadResult:
        self.request = request
        return self.result


class _QualityEvaluator:
    def __init__(self, evaluation: QualityEvaluation) -> None:
        self.evaluation = evaluation
        self.dataframe: _DataFrame | None = None
        self.run_id: str | None = None

    def evaluate(self, dataframe: _DataFrame, *, run_id: str) -> QualityEvaluation:
        self.dataframe = dataframe
        self.run_id = run_id
        return self.evaluation


class _ReconciliationEvaluator:
    def __init__(self, result: ReconciliationResult) -> None:
        self.result = result
        self.evidence = None
        self.run_id: str | None = None

    def evaluate(self, evidence: object, *, run_id: str) -> ReconciliationResult:
        self.evidence = evidence
        self.run_id = run_id
        return self.result


class _TableListAuditManager(TableDeliveryAuditManager):
    def __init__(self) -> None:
        self.deliveries = (
            TableDelivery(
                "pipeline-42",
                "bronze-customer",
                "101",
                "share.crm.customer",
                data_quality_rule_refs=("dq-customer-not-null",),
            ),
            TableDelivery(
                "pipeline-42",
                "bronze-country",
                "102",
                "share.crm.country",
                data_quality_rule_refs=("dq-country-code",),
            ),
        )
        self.outcomes: list[TableDeliveryOutcome] = []

    def load_table_deliveries(self, pipeline_run_id: str) -> tuple[TableDelivery, ...]:
        if pipeline_run_id != "pipeline-42":
            raise LookupError(pipeline_run_id)
        return self.deliveries

    def record_quality_result(self, delivery: TableDelivery, result: object) -> None:
        return None

    def record_reconciliation_result(self, delivery: TableDelivery, result: object) -> None:
        return None

    def record_table_outcome(self, outcome: TableDeliveryOutcome) -> None:
        self.outcomes.append(outcome)


class _TableListIngestionMethod(TableIngestionMethod):
    def __init__(self, runtime_factory: _TableListRuntimeFactory) -> None:
        self._runtime_factory = runtime_factory

    def execute(
        self,
        delivery: TableDelivery,
        *,
        quality_evaluator: object | None,
        reconciliation_evaluator: object | None,
    ) -> TableDeliveryEvidence:
        self._runtime_factory.executed_sources.append(delivery.source_object)
        return TableDeliveryEvidence()


class _TableListRuntimeFactory(DeltaSharingTableRuntimeFactory):
    def __init__(self) -> None:
        super().__init__(spark=object(), delta_sharing_profile="profile.json")
        self.executed_sources: list[str] = []
        self.quality_rule_sets: list[tuple[str, ...]] = []

    def create_ingestion_method(self, delivery: TableDelivery) -> TableIngestionMethod:
        return _TableListIngestionMethod(self)

    def create_data_quality_evaluator(self, delivery: TableDelivery) -> object:
        self.quality_rule_sets.append(delivery.data_quality_rule_refs)
        return object()

    def create_reconciliation_evaluator(self, delivery: TableDelivery) -> None:
        return None


class DeltaSharingControlledIngestionTests(unittest.TestCase):
    def test_execution_group_constructs_components_for_every_frozen_delivery(self) -> None:
        audit_manager = _TableListAuditManager()
        runtime_factory = _TableListRuntimeFactory()

        outcome = ingest_execution_group(
            "mssql+pyodbc://example",
            runtime_factory,
            {"PIPELINE_RUN_ID": "pipeline-42"},
            audit_manager=audit_manager,
        )

        self.assertEqual(
            runtime_factory.executed_sources,
            ["share.crm.customer", "share.crm.country"],
        )
        self.assertEqual(
            runtime_factory.quality_rule_sets,
            [("dq-customer-not-null",), ("dq-country-code",)],
        )
        self.assertEqual(len(audit_manager.outcomes), 2)
        self.assertEqual(outcome.status, ExecutionGroupStatus.SUCCEEDED)

    def test_settings_require_published_rows_for_reconciliation(self) -> None:
        with self.assertRaisesRegex(ValueError, "BRONZE_PUBLISHED_ROWS"):
            DeltaSharingBronzeDeliverySettings.from_environment(
                {
                    "CONTROL_PLANE_SQLALCHEMY_URL": "mssql+pyodbc://example",
                    "DELTA_SHARING_PROFILE": "profile.json",
                    "BRONZE_RUN_ID": "bronze-42",
                    "BRONZE_DELIVERY_ID": "delivery-42",
                    "BRONZE_COMPLETION_STATE": "COMPLETE",
                }
            )

    def test_ingest_uses_the_frozen_target_and_records_the_delivery(self) -> None:
        dataframe = _DataFrame()
        plan = FrozenBronzeReaderPlan(
            bronze_run_id="bronze-42",
            source_to_bronze_config_id="101",
            bronze_relation_ref="bronze.customer_v1",
            source_boundary_ref="snapshot://crm/17",
            source_read_plan=SourceReadPlan(
                source_object="share.crm.customer",
                capture_mode=SourceCaptureMode.FULL,
            ),
        )
        manager = _Manager(plan)
        reader = _Reader(
            SourceReadResult(
                dataframe=dataframe,
                evidence=SourceReadEvidence(
                    source_object="share.crm.customer",
                    capture_mode=SourceCaptureMode.FULL,
                    boundary=SnapshotReadBoundary(source_version="17"),
                    source_row_count=12,
                ),
            )
        )
        settings = DeltaSharingBronzeDeliverySettings(
            control_plane_connection_url="mssql+pyodbc://example",
            delta_sharing_profile="profile.json",
            bronze_run_id="bronze-42",
            delivery_id="delivery-42",
            completion_state=BronzeCompletionState.COMPLETE,
            delta_version=9,
            published_rows=12,
        )

        evidence = ingest_delta_sharing_delivery(  # type: ignore[arg-type]
            manager,
            reader,
            settings,
            {"SOURCE_VERSION": "17"},
        )

        self.assertEqual(manager.requested_run_id, "bronze-42")
        self.assertIsNotNone(reader.request)
        assert reader.request is not None
        self.assertEqual(reader.request.boundary, SnapshotReadBoundary(source_version="17"))
        self.assertEqual(dataframe.write.table_name, "bronze.customer_v1")
        self.assertEqual(manager.publication.source_boundary_ref, "snapshot://crm/17")
        self.assertEqual(manager.publication.published_rows, 12)
        self.assertEqual(evidence.source_row_count, 12)

    def test_ingest_evaluates_quality_and_reconciles_before_recording_manifest(self) -> None:
        dataframe = _DataFrame()
        plan = FrozenBronzeReaderPlan(
            bronze_run_id="bronze-42",
            source_to_bronze_config_id="101",
            bronze_relation_ref="bronze.customer_v1",
            source_boundary_ref="snapshot://crm/17",
            source_read_plan=SourceReadPlan(
                source_object="share.crm.customer",
                capture_mode=SourceCaptureMode.FULL,
            ),
        )
        manager = _Manager(plan)
        reader = _Reader(
            SourceReadResult(
                dataframe=dataframe,
                evidence=SourceReadEvidence(
                    source_object="share.crm.customer",
                    capture_mode=SourceCaptureMode.FULL,
                    boundary=SnapshotReadBoundary(source_version="17"),
                    source_row_count=12,
                ),
            )
        )
        quality_evaluator = _QualityEvaluator(
            QualityEvaluation(
                valid_dataframe=dataframe,
                quarantine_dataframe=None,
                result=QualityRunResult(
                    run_id="bronze-42",
                    overall_status="PASSED",
                    rule_results=(),
                    evaluated_rows=12,
                ),
            )
        )
        reconciliation_evaluator = _ReconciliationEvaluator(
            ReconciliationResult(
                run_id="bronze-42",
                passed=True,
                rule_results=(
                    ReconciliationRuleResult("row-count-balance", passed=True),
                ),
            )
        )
        settings = DeltaSharingBronzeDeliverySettings(
            control_plane_connection_url="mssql+pyodbc://example",
            delta_sharing_profile="profile.json",
            bronze_run_id="bronze-42",
            delivery_id="delivery-42",
            completion_state=BronzeCompletionState.COMPLETE,
            published_rows=12,
        )

        ingest_delta_sharing_delivery(
            manager,
            reader,  # type: ignore[arg-type]
            settings,
            {"SOURCE_VERSION": "17"},
            quality_evaluator=quality_evaluator,  # type: ignore[arg-type]
            reconciliation_evaluator=reconciliation_evaluator,  # type: ignore[arg-type]
        )

        self.assertIs(quality_evaluator.dataframe, dataframe)
        self.assertEqual(quality_evaluator.run_id, "bronze-42")
        self.assertIsNotNone(reconciliation_evaluator.evidence)
        self.assertEqual(reconciliation_evaluator.run_id, "bronze-42")
        self.assertIsNotNone(manager.publication)

    def test_ingest_does_not_publish_when_quality_fails(self) -> None:
        dataframe = _DataFrame()
        plan = FrozenBronzeReaderPlan(
            bronze_run_id="bronze-42",
            source_to_bronze_config_id="101",
            bronze_relation_ref="bronze.customer_v1",
            source_boundary_ref="snapshot://crm/17",
            source_read_plan=SourceReadPlan(
                source_object="share.crm.customer",
                capture_mode=SourceCaptureMode.FULL,
            ),
        )
        manager = _Manager(plan)
        reader = _Reader(
            SourceReadResult(
                dataframe=dataframe,
                evidence=SourceReadEvidence(
                    source_object="share.crm.customer",
                    capture_mode=SourceCaptureMode.FULL,
                    boundary=SnapshotReadBoundary(source_version="17"),
                ),
            )
        )
        quality_evaluator = _QualityEvaluator(
            QualityEvaluation(
                valid_dataframe=dataframe,
                quarantine_dataframe=None,
                result=QualityRunResult(
                    run_id="bronze-42",
                    overall_status="FAILED",
                    rule_results=(),
                ),
            )
        )
        settings = DeltaSharingBronzeDeliverySettings(
            control_plane_connection_url="mssql+pyodbc://example",
            delta_sharing_profile="profile.json",
            bronze_run_id="bronze-42",
            delivery_id="delivery-42",
            completion_state=BronzeCompletionState.COMPLETE,
        )

        with self.assertRaisesRegex(RuntimeError, "data-quality"):
            ingest_delta_sharing_delivery(
                manager,
                reader,  # type: ignore[arg-type]
                settings,
                {"SOURCE_VERSION": "17"},
                quality_evaluator=quality_evaluator,  # type: ignore[arg-type]
            )

        self.assertIsNone(dataframe.write.table_name)
        self.assertIsNone(manager.publication)


if __name__ == "__main__":
    unittest.main()