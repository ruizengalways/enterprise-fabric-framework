from __future__ import annotations

import unittest

from enterprise_fabric_framework.orchestration import (
    ExecutionGroupRunner,
    ExecutionGroupStatus,
    RuntimeConfiguredTableDeliveryExecutor,
    TableDelivery,
    TableDeliveryAuditManager,
    TableDeliveryEvidence,
    TableDeliveryExecutor,
    TableDeliveryRuntimeFactory,
    TableDeliveryStatus,
    TableIngestionMethod,
)
from enterprise_fabric_framework.quality import DataQualityStatus, QualityRunResult
from enterprise_fabric_framework.reconciliation import ReconciliationResult, ReconciliationStatus


class _AuditManager(TableDeliveryAuditManager):
    def __init__(self, deliveries: tuple[TableDelivery, ...]) -> None:
        self.deliveries = deliveries
        self.quality_results: list[QualityRunResult] = []
        self.reconciliation_results: list[ReconciliationResult] = []
        self.outcomes = []

    def load_table_deliveries(self, pipeline_run_id: str) -> tuple[TableDelivery, ...]:
        if pipeline_run_id != "pipeline-42":
            raise LookupError(pipeline_run_id)
        return self.deliveries

    def record_quality_result(
        self,
        delivery: TableDelivery,
        result: QualityRunResult,
    ) -> None:
        self.quality_results.append(result)

    def record_reconciliation_result(
        self,
        delivery: TableDelivery,
        result: ReconciliationResult,
    ) -> None:
        self.reconciliation_results.append(result)

    def record_table_outcome(self, outcome: object) -> None:
        self.outcomes.append(outcome)


class _Executor(TableDeliveryExecutor):
    def __init__(self, evidence: dict[str, TableDeliveryEvidence], failures: set[str] = set()) -> None:
        self._evidence = evidence
        self._failures = failures
        self.executed_run_ids: list[str] = []

    def execute(self, delivery: TableDelivery) -> TableDeliveryEvidence:
        self.executed_run_ids.append(delivery.bronze_run_id)
        if delivery.bronze_run_id in self._failures:
            raise RuntimeError("source unavailable")
        return self._evidence[delivery.bronze_run_id]


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
        self._factory.executed.append(
            (
                delivery.bronze_run_id,
                quality_evaluator,
                reconciliation_evaluator,
            )
        )
        return TableDeliveryEvidence()


class _RuntimeFactory(TableDeliveryRuntimeFactory):
    def __init__(self) -> None:
        self.constructed: list[tuple[str, tuple[str, ...], tuple[str, ...]]] = []
        self.executed: list[tuple[str, object | None, object | None]] = []

    def create_ingestion_method(self, delivery: TableDelivery) -> TableIngestionMethod:
        self.constructed.append(
            (
                delivery.ingestion_method_ref,
                delivery.data_quality_rule_refs,
                delivery.reconciliation_rule_refs,
            )
        )
        return _IngestionMethod(self)

    def create_data_quality_evaluator(self, delivery: TableDelivery) -> object:
        return ("dq", delivery.data_quality_rule_refs)

    def create_reconciliation_evaluator(self, delivery: TableDelivery) -> object:
        return ("recon", delivery.reconciliation_rule_refs)


class ExecutionGroupRunnerTests(unittest.TestCase):
    def test_continues_after_one_table_fails_and_records_every_outcome(self) -> None:
        deliveries = (
            _delivery("bronze-customer", "crm.customer"),
            _delivery("bronze-country", "crm.country"),
            _delivery("bronze-product", "crm.product"),
        )
        audit_manager = _AuditManager(deliveries)
        executor = _Executor(
            {
                "bronze-customer": TableDeliveryEvidence(
                    quality_result=QualityRunResult(
                        run_id="bronze-customer",
                        overall_status=DataQualityStatus.PASSED,
                        rule_results=(),
                    )
                ),
                "bronze-country": TableDeliveryEvidence(
                    reconciliation_result=ReconciliationResult(
                        run_id="bronze-country",
                        passed=False,
                        rule_results=(),
                        status=ReconciliationStatus.FAILED,
                    )
                ),
                "bronze-product": TableDeliveryEvidence(
                    quality_result=QualityRunResult(
                        run_id="bronze-product",
                        overall_status=DataQualityStatus.WARNING,
                        rule_results=(),
                    )
                ),
            }
        )

        outcome = ExecutionGroupRunner(audit_manager, executor).run("pipeline-42")

        self.assertEqual(executor.executed_run_ids, [
            "bronze-customer",
            "bronze-country",
            "bronze-product",
        ])
        self.assertEqual(outcome.status, ExecutionGroupStatus.PARTIAL_FAILURE)
        self.assertEqual(
            [item.status for item in outcome.table_outcomes],
            [
                TableDeliveryStatus.SUCCEEDED,
                TableDeliveryStatus.FAILED,
                TableDeliveryStatus.SUCCEEDED_WITH_WARNINGS,
            ],
        )
        self.assertEqual(len(audit_manager.quality_results), 2)
        self.assertEqual(len(audit_manager.reconciliation_results), 1)
        self.assertEqual(len(audit_manager.outcomes), 3)

    def test_continues_after_an_unexpected_table_execution_error(self) -> None:
        deliveries = (
            _delivery("bronze-customer", "crm.customer"),
            _delivery("bronze-country", "crm.country"),
        )
        audit_manager = _AuditManager(deliveries)
        executor = _Executor(
            {"bronze-country": TableDeliveryEvidence()},
            failures={"bronze-customer"},
        )

        outcome = ExecutionGroupRunner(audit_manager, executor).run("pipeline-42")

        self.assertEqual(executor.executed_run_ids, ["bronze-customer", "bronze-country"])
        self.assertEqual(outcome.table_outcomes[0].error_code, "RuntimeError")
        self.assertEqual(outcome.table_outcomes[1].status, TableDeliveryStatus.SUCCEEDED)

    def test_constructs_runtime_components_for_each_table_delivery(self) -> None:
        factory = _RuntimeFactory()
        executor = RuntimeConfiguredTableDeliveryExecutor(factory)
        customer = TableDelivery(
            "pipeline-42",
            "bronze-customer",
            "101",
            "crm.customer",
            ingestion_method_ref="delta-sharing@1",
            data_quality_rule_refs=("customer-not-null@1",),
            reconciliation_rule_refs=("customer-count@1",),
        )
        country = TableDelivery(
            "pipeline-42",
            "bronze-country",
            "102",
            "crm.country",
            ingestion_method_ref="jdbc@1",
            data_quality_rule_refs=("country-not-null@1",),
            reconciliation_rule_refs=("country-count@1",),
        )

        executor.execute(customer)
        executor.execute(country)

        self.assertEqual(
            factory.constructed,
            [
                ("delta-sharing@1", ("customer-not-null@1",), ("customer-count@1",)),
                ("jdbc@1", ("country-not-null@1",), ("country-count@1",)),
            ],
        )
        self.assertEqual(factory.executed[0][1], ("dq", ("customer-not-null@1",)))
        self.assertEqual(factory.executed[1][2], ("recon", ("country-count@1",)))


def _delivery(bronze_run_id: str, source_object: str) -> TableDelivery:
    return TableDelivery(
        pipeline_run_id="pipeline-42",
        bronze_run_id=bronze_run_id,
        source_to_bronze_config_id="101",
        source_object=source_object,
    )


if __name__ == "__main__":
    unittest.main()