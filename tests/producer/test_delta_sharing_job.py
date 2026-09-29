from __future__ import annotations

import unittest

from enterprise_fabric_framework.control_plane import BronzeCompletionState
from enterprise_fabric_framework.orchestration import (
    TableDelivery,
    TableDeliveryAuditManager,
    TableDeliveryEvidence,
    TableDeliveryOutcome,
    TableDeliveryRuntimeFactory,
    TableIngestionMethod,
)
from enterprise_fabric_framework.producer.delta_sharing_job import (
    BronzeDeliverySettings,
    DeltaSharingBronzeDeliverySettings,
    DeltaSharingExecutionGroupJob,
    DeltaSharingTableRuntimeFactory,
)


class _AuditManager(TableDeliveryAuditManager):
    def __init__(self) -> None:
        self.deliveries = (
            TableDelivery("pipeline-42", "bronze-customer", "101", "share.crm.customer"),
            TableDelivery("pipeline-42", "bronze-country", "102", "share.crm.country"),
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


class _IngestionMethod(TableIngestionMethod):
    def __init__(self, factory: _RuntimeFactory) -> None:
        self._factory = factory

    def execute(
        self,
        delivery: TableDelivery,
        *,
        quality_evaluator: object | None,
        reconciliation_evaluator: object | None,
    ) -> TableDeliveryEvidence:
        self._factory.executed.append(delivery.source_object)
        return TableDeliveryEvidence()


class _RuntimeFactory(DeltaSharingTableRuntimeFactory):
    def __init__(self) -> None:
        super().__init__(spark=object(), delta_sharing_profile="profile.json")
        self.executed: list[str] = []

    def create_ingestion_method(self, delivery: TableDelivery) -> TableIngestionMethod:
        return _IngestionMethod(self)

    def create_data_quality_evaluator(self, delivery: TableDelivery) -> None:
        return None

    def create_reconciliation_evaluator(self, delivery: TableDelivery) -> None:
        return None


class DeltaSharingJobTests(unittest.TestCase):
    def test_delta_sharing_settings_extend_the_common_delivery_settings(self) -> None:
        settings = DeltaSharingBronzeDeliverySettings(
            control_plane_connection_url="mssql+pyodbc://example",
            delta_sharing_profile="profile.json",
            bronze_run_id="bronze-42",
            delivery_id="delivery-42",
            completion_state=BronzeCompletionState.COMPLETE,
            published_rows=12,
        )

        self.assertIsInstance(settings, BronzeDeliverySettings)
        self.assertEqual(settings.delta_sharing_profile, "profile.json")
        self.assertEqual(settings.published_rows, 12)

    def test_execution_group_job_runs_each_delivery_with_the_delta_sharing_factory(self) -> None:
        audit_manager = _AuditManager()
        runtime_factory = _RuntimeFactory()
        job = DeltaSharingExecutionGroupJob(
            "mssql+pyodbc://example",
            runtime_factory,
            audit_manager=audit_manager,
        )

        outcome = job.run({"PIPELINE_RUN_ID": "pipeline-42"})

        self.assertEqual(
            runtime_factory.executed,
            ["share.crm.customer", "share.crm.country"],
        )
        self.assertEqual(len(audit_manager.outcomes), 2)
        self.assertEqual(outcome.pipeline_run_id, "pipeline-42")


if __name__ == "__main__":
    unittest.main()