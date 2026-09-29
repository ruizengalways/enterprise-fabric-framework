from __future__ import annotations

import unittest
from collections.abc import Mapping
from typing import Any

from enterprise_fabric_framework.connector import (
    ConnectionHealth,
    ConnectionHealthStatus,
    Connector,
    FabricSqlDatabase,
)
from enterprise_fabric_framework.control_plane import (
    BronzeCompletionState,
    BronzeControlPlaneManager,
    BronzePublication,
    ControlPlaneManager,
    SilverCompletionState,
    SilverControlPlaneManager,
    SilverMicroBatch,
)
from enterprise_fabric_framework.producer.readers import (
    SnapshotReadBoundary,
    SourceCaptureMode,
    SourceReadEvidence,
    SourceReadRequest,
    SourceReadResult,
)


class _Result:
    def __init__(self, row: Mapping[str, object] | None = None, rowcount: int = 1) -> None:
        self._row = row
        self.rowcount = rowcount

    def mappings(self) -> _Result:
        return self

    def one_or_none(self) -> Mapping[str, object] | None:
        return self._row


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
        return _Result(self._database._row)


class _Database(Connector):
    def __init__(self, row: Mapping[str, object]) -> None:
        self._row = row
        self.fetches: list[tuple[str, Mapping[str, object]]] = []
        self.commands: list[tuple[str, Mapping[str, object]]] = []

    def connect(self) -> _Connection:
        return _Connection(self, commands=False)

    def begin(self) -> _Connection:
        return _Connection(self, commands=True)

    def health(self) -> ConnectionHealth:
        return ConnectionHealth(type(self).__name__, ConnectionHealthStatus.HEALTHY)


class _Reader:
    def __init__(self, result: SourceReadResult) -> None:
        self.result = result
        self.request: SourceReadRequest | None = None

    def read(self, request: SourceReadRequest) -> SourceReadResult:
        self.request = request
        return self.result


class ControlPlaneManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.database = _Database(
            {
                "bronze_run_id": "bronze-42",
                "source_to_bronze_config_id": "101",
                "bronze_relation_ref": "bronze.customer_v1",
                "source_boundary_ref": "snapshot://crm/17",
                "source_object_ref": "share.crm.customer",
                "capture_mode": "FULL",
                "watermark_column": None,
            }
        )
        self.bronze_manager = BronzeControlPlaneManager(self.database)
        self.silver_manager = SilverControlPlaneManager(self.database)

    def test_read_passes_the_frozen_plan_to_the_reader(self) -> None:
        result = SourceReadResult(
            dataframe=object(),
            evidence=SourceReadEvidence(
                source_object="share.crm.customer",
                capture_mode=SourceCaptureMode.FULL,
                boundary=SnapshotReadBoundary(source_version="17"),
                source_row_count=12,
            ),
        )
        reader = _Reader(result)

        actual = self.bronze_manager.read(
            "bronze-42",
            reader,  # type: ignore[arg-type]
            SnapshotReadBoundary(source_version="17"),
        )

        self.assertIs(actual, result)
        self.assertIsInstance(self.bronze_manager, ControlPlaneManager)
        self.assertTrue(issubclass(FabricSqlDatabase, Connector))
        self.assertIsNotNone(reader.request)
        assert reader.request is not None
        self.assertEqual(reader.request.plan.source_object, "share.crm.customer")
        self.assertEqual(reader.request.plan.capture_mode, SourceCaptureMode.FULL)
        self.assertEqual(reader.request.boundary, SnapshotReadBoundary(source_version="17"))
        self.assertIn("usp_get_bronze_reader_plan", self.database.fetches[0][0])
        self.assertEqual(self.database.fetches[0][1], {"bronze_run_id": "bronze-42"})

    def test_manifest_records_reader_evidence_without_the_dataframe(self) -> None:
        read_result = SourceReadResult(
            dataframe=object(),
            evidence=SourceReadEvidence(
                source_object="share.crm.customer",
                capture_mode=SourceCaptureMode.FULL,
                boundary=SnapshotReadBoundary(source_version="17"),
                source_row_count=12,
            ),
        )

        self.bronze_manager.record_bronze_manifest(
            BronzePublication(
                delivery_id="delivery-42",
                bronze_run_id="bronze-42",
                source_to_bronze_config_id="101",
                bronze_relation_ref="bronze.customer_v1",
                completion_state=BronzeCompletionState.COMPLETE,
                source_boundary_ref="snapshot://crm/17",
                delta_version=9,
                published_rows=12,
            ),
            read_result,
        )

        statement, parameters = self.database.commands[0]
        self.assertIn("usp_record_bronze_manifest", statement)
        self.assertEqual(parameters["selected_rows"], 12)
        self.assertEqual(parameters["published_rows"], 12)
        self.assertNotIn("dataframe", parameters)

    def test_loads_a_typed_frozen_silver_consumer_plan(self) -> None:
        self.database._row = {
            "silver_run_id": "silver-42",
            "bronze_to_silver_config_id": "201",
            "source_to_bronze_config_id": "101",
            "bronze_relation_ref": "bronze.customer_v1",
            "silver_relation_ref": "silver.customer_v1",
            "checkpoint_ref": "checkpoints/customer-v1",
            "query_identity": "customer-v1-generation-1",
            "load_strategy": "SCD1",
            "policy_schema_version": 1,
            "schema_ref": "schemas.customer@1",
            "schema_columns": '[{"name":"customer_id","type":"long","nullable":false}]',
            "schema_mode": "STRICT",
            "entity_key_columns": '["customer_id"]',
            "event_identity_columns": '[]',
            "ordering_columns": '["modified_at", "change_id"]',
            "content_hash_ref": "canonical_sha256@1",
            "content_columns": '["customer_id", "name"]',
            "source_operation_column": "operation",
            "delete_operation_values": '["D"]',
            "delete_marker_column": None,
            "delete_marker_values": '[]',
            "delete_action": "IGNORE",
            "effective_time_column": None,
            "tracked_columns": '[]',
            "late_arrival_policy": None,
            "correction_window_seconds": None,
            "snapshot_selection_ref": None,
            "snapshot_diff_apply_ref": None,
            "replace_publication_ref": None,
        }

        plan = self.silver_manager.load_silver_consumer_plan("silver-42")

        self.assertIsInstance(self.silver_manager, ControlPlaneManager)
        self.assertEqual(plan.bronze_relation_ref, "bronze.customer_v1")
        self.assertEqual(plan.silver_relation_ref, "silver.customer_v1")
        self.assertEqual(plan.load_strategy, "SCD1")
        self.assertEqual(plan.policy.entity_key_columns, ("customer_id",))
        self.assertEqual(plan.policy.ordering_columns, ("modified_at", "change_id"))
        self.assertEqual(plan.policy.schema_columns[0]["name"], "customer_id")
        self.assertIn("usp_get_silver_consumer_plan", self.database.fetches[-1][0])

    def test_silver_manifest_records_bounded_micro_batch_evidence(self) -> None:
        self.silver_manager.record_silver_manifest(
            SilverMicroBatch(
                query_identity="customer-v1-generation-1",
                batch_id=42,
                silver_run_id="silver-42",
                completion_state=SilverCompletionState.COMPLETE,
                input_rows=12,
                accepted_rows=11,
                quarantined_rows=1,
                inserted_rows=8,
                updated_rows=3,
                target_commit_ref="delta://silver/customer#42",
                reconciliation_ref="evidence://reconciliation/42",
            )
        )

        statement, parameters = self.database.commands[-1]
        self.assertIn("usp_record_silver_manifest", statement)
        self.assertEqual(parameters["batch_id"], 42)
        self.assertEqual(parameters["accepted_rows"], 11)
        self.assertEqual(parameters["completion_state"], "COMPLETE")


if __name__ == "__main__":
    unittest.main()