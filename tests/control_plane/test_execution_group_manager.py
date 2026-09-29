from __future__ import annotations

import unittest
from collections.abc import Mapping

from enterprise_fabric_framework.connector import ConnectionHealth, ConnectionHealthStatus, Connector
from enterprise_fabric_framework.control_plane import ExecutionGroupControlPlaneManager
from enterprise_fabric_framework.orchestration import TableDeliveryOutcome, TableDeliveryStatus
from enterprise_fabric_framework.quality import (
    DataQualitySeverity,
    DataQualityStatus,
    QualityRuleResult,
    QualityRunResult,
)
from enterprise_fabric_framework.reconciliation import (
    ReconciliationResult,
    ReconciliationRuleResult,
    ReconciliationSeverity,
    ReconciliationStatus,
)


class _Result:
    def __init__(self, rows: tuple[Mapping[str, object], ...] = ()) -> None:
        self._rows = rows
        self.rowcount = 1

    def mappings(self) -> _Result:
        return self

    def all(self) -> tuple[Mapping[str, object], ...]:
        return self._rows


class _Connection:
    def __init__(self, database: _Database, commands: bool) -> None:
        self._database = database
        self._commands = commands

    def __enter__(self) -> _Connection:
        return self

    def __exit__(self, *arguments: object) -> None:
        return None

    def execute(self, statement: str, parameters: Mapping[str, object]) -> _Result:
        if self._commands:
            self._database.commands.append((statement, parameters))
            return _Result()
        self._database.fetches.append((statement, parameters))
        return _Result(self._database.rows)


class _Database(Connector):
    def __init__(self) -> None:
        self.rows: tuple[Mapping[str, object], ...] = (
            {
                "bronze_run_id": "bronze-customer",
                "source_to_bronze_config_id": "101",
                "source_object_ref": "share.crm.customer",
                "ingestion_method_ref": "delta-sharing@1",
                "data_quality_rule_refs": '["customer-not-null@1"]',
                "reconciliation_rule_refs": '["customer-count@1"]',
            },
            {
                "bronze_run_id": "bronze-country",
                "source_to_bronze_config_id": "102",
                "source_object_ref": "share.crm.country",
                "ingestion_method_ref": "jdbc@1",
                "data_quality_rule_refs": '["country-not-null@1"]',
                "reconciliation_rule_refs": '["country-count@1"]',
            },
        )
        self.fetches: list[tuple[str, Mapping[str, object]]] = []
        self.commands: list[tuple[str, Mapping[str, object]]] = []

    def connect(self) -> _Connection:
        return _Connection(self, commands=False)

    def begin(self) -> _Connection:
        return _Connection(self, commands=True)

    def health(self) -> ConnectionHealth:
        return ConnectionHealth(type(self).__name__, ConnectionHealthStatus.HEALTHY)


class ExecutionGroupControlPlaneManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.database = _Database()
        self.manager = ExecutionGroupControlPlaneManager(self.database)

    def test_loads_each_frozen_delivery_with_its_runtime_references(self) -> None:
        deliveries = self.manager.load_table_deliveries("pipeline-42")

        self.assertEqual([delivery.bronze_run_id for delivery in deliveries], [
            "bronze-customer",
            "bronze-country",
        ])
        self.assertEqual(deliveries[0].ingestion_method_ref, "delta-sharing@1")
        self.assertEqual(deliveries[1].data_quality_rule_refs, ("country-not-null@1",))
        self.assertEqual(deliveries[1].reconciliation_rule_refs, ("country-count@1",))
        self.assertIn("usp_get_execution_group_bronze_deliveries", self.database.fetches[0][0])

    def test_records_per_rule_and_terminal_table_audit_evidence(self) -> None:
        delivery = self.manager.load_table_deliveries("pipeline-42")[0]
        self.manager.record_quality_result(
            delivery,
            QualityRunResult(
                run_id=delivery.bronze_run_id,
                overall_status=DataQualityStatus.WARNING,
                rule_results=(
                    QualityRuleResult(
                        "customer-not-null",
                        "FAILED",
                        11,
                        1,
                        severity=DataQualitySeverity.WARNING,
                    ),
                ),
            ),
        )
        self.manager.record_reconciliation_result(
            delivery,
            ReconciliationResult(
                run_id=delivery.bronze_run_id,
                passed=True,
                status=ReconciliationStatus.WARNING,
                rule_results=(
                    ReconciliationRuleResult(
                        "customer-count",
                        False,
                        expected_rows=12,
                        actual_rows=11,
                        severity=ReconciliationSeverity.WARNING,
                    ),
                ),
            ),
        )
        self.manager.record_table_outcome(
            TableDeliveryOutcome(delivery, TableDeliveryStatus.SUCCEEDED_WITH_WARNINGS)
        )

        statements = [statement for statement, _ in self.database.commands]
        self.assertEqual(len(statements), 4)
        self.assertIn("usp_record_bronze_dq_result", statements[0])
        self.assertIn("usp_record_bronze_recon_rule_result", statements[1])
        self.assertIn("usp_record_bronze_recon_result", statements[2])
        self.assertIn("usp_record_bronze_run_outcome", statements[3])
        self.assertEqual(self.database.commands[0][1]["severity"], "WARNING")
        self.assertEqual(self.database.commands[2][1]["status"], "WARNING")


if __name__ == "__main__":
    unittest.main()