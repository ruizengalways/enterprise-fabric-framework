from __future__ import annotations

import unittest

from enterprise_fabric_framework.orchestration import (
    ExecutionGroupJob,
    ExecutionGroupStatus,
    TableDelivery,
    TableDeliveryAuditManager,
    TableDeliveryEvidence,
    TableDeliveryExecutor,
    TableDeliveryOutcome,
)


class _AuditManager(TableDeliveryAuditManager):
    def __init__(self) -> None:
        self.deliveries = (
            TableDelivery("pipeline-42", "bronze-customer", "101", "crm.customer"),
            TableDelivery("pipeline-42", "bronze-country", "102", "crm.country"),
        )
        self.outcomes: list[TableDeliveryOutcome] = []

    def load_table_deliveries(self, pipeline_run_id: str) -> tuple[TableDelivery, ...]:
        self.assert_pipeline_run_id(pipeline_run_id)
        return self.deliveries

    def record_quality_result(self, delivery: TableDelivery, result: object) -> None:
        return None

    def record_reconciliation_result(self, delivery: TableDelivery, result: object) -> None:
        return None

    def record_table_outcome(self, outcome: TableDeliveryOutcome) -> None:
        self.outcomes.append(outcome)

    @staticmethod
    def assert_pipeline_run_id(pipeline_run_id: str) -> None:
        if pipeline_run_id != "pipeline-42":
            raise LookupError(pipeline_run_id)


class _Executor(TableDeliveryExecutor):
    def __init__(self) -> None:
        self.executed_table_names: list[str] = []

    def execute(self, delivery: TableDelivery) -> TableDeliveryEvidence:
        self.executed_table_names.append(delivery.source_object)
        return TableDeliveryEvidence()


class _Job(ExecutionGroupJob):
    def __init__(self) -> None:
        self.audit_manager = _AuditManager()
        self.table_executor = _Executor()

    def create_audit_manager(self) -> TableDeliveryAuditManager:
        return self.audit_manager

    def create_table_executor(self) -> TableDeliveryExecutor:
        return self.table_executor


class ExecutionGroupExampleTests(unittest.TestCase):
    def test_job_processes_every_table_in_the_control_plane_list(self) -> None:
        job = _Job()

        outcome = job.run({"PIPELINE_RUN_ID": "pipeline-42"})

        self.assertEqual(job.table_executor.executed_table_names, ["crm.customer", "crm.country"])
        self.assertEqual(len(job.audit_manager.outcomes), 2)
        self.assertEqual(outcome.status, ExecutionGroupStatus.SUCCEEDED)

    def test_job_requires_a_parent_pipeline_run_id(self) -> None:
        with self.assertRaisesRegex(ValueError, "PIPELINE_RUN_ID"):
            _Job().run({})


if __name__ == "__main__":
    unittest.main()