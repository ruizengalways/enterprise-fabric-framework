from __future__ import annotations

import unittest

from enterprise_fabric_framework.connector.connectors import (
    ConnectionHealth,
    ConnectionHealthStatus,
    Connector,
)
from enterprise_fabric_framework.connector.delta_sharing_connector import DeltaSharingConnector
from enterprise_fabric_framework.producer.readers.delta_sharing_reader import DeltaSharingReader
from enterprise_fabric_framework.producer.readers.reader import (
    SnapshotReadBoundary,
    SnapshotSourceReader,
    SourceCaptureMode,
    SourceReadPlan,
    SourceReadRequest,
    SourceReader,
    WatermarkReadBoundary,
)


class _Predicate:
    def __init__(self, value: str) -> None:
        self.value = value

    def __and__(self, other: _Predicate) -> _Predicate:
        return _Predicate(f"({self.value}) AND ({other.value})")


class _Column:
    def __init__(self, name: str) -> None:
        self._name = name

    def __ge__(self, value: object) -> _Predicate:
        return _Predicate(f"{self._name} >= {value}")

    def __lt__(self, value: object) -> _Predicate:
        return _Predicate(f"{self._name} < {value}")


class _DataFrame:
    def __init__(self) -> None:
        self.predicate: _Predicate | None = None
        self.limit_value: int | None = None
        self.count_called = False

    def __getitem__(self, column_name: str) -> _Column:
        return _Column(column_name)

    def where(self, predicate: _Predicate) -> _DataFrame:
        self.predicate = predicate
        return self

    def limit(self, limit_value: int) -> _DataFrame:
        self.limit_value = limit_value
        return self

    def count(self) -> int:
        self.count_called = True
        return 0


class _SparkReader:
    def __init__(self, dataframe: _DataFrame) -> None:
        self._dataframe = dataframe
        self.format_name: str | None = None
        self.loaded_path: str | None = None

    def format(self, format_name: str) -> _SparkReader:
        self.format_name = format_name
        return self

    def load(self, path: str) -> _DataFrame:
        self.loaded_path = path
        return self._dataframe


class _SparkSession:
    def __init__(self, dataframe: _DataFrame) -> None:
        self.read = _SparkReader(dataframe)


class _Connector(Connector):
    def __init__(self, dataframe: _DataFrame) -> None:
        self.dataframe = dataframe
        self.table_name: str | None = None

    def read(self, table_name: str) -> _DataFrame:
        self.table_name = table_name
        return self.dataframe

    def health(self) -> ConnectionHealth:
        return ConnectionHealth(type(self).__name__, ConnectionHealthStatus.HEALTHY)


class _WatermarkReader(SourceReader):
    supports_watermark = True

    def read(self, request: SourceReadRequest):
        return self.read_from_request(request)


class SourceReadRequestTests(unittest.TestCase):
    def test_delta_sharing_reader_uses_the_delta_sharing_connector(self) -> None:
        dataframe = _DataFrame()
        spark = _SparkSession(dataframe)
        reader = DeltaSharingReader(DeltaSharingConnector(spark, "profile.json"))
        request = SourceReadRequest.for_watermark(
            SourceReadPlan(
                source_object="share.schema.customer",
                capture_mode=SourceCaptureMode.WATERMARK,
                watermark_column="modified_at",
            ),
            lower_bound="2026-09-24T00:00:00Z",
            upper_bound="2026-09-25T00:00:00Z",
        )

        result = reader.read(request)

        self.assertIs(result.dataframe, dataframe)
        self.assertEqual(spark.read.format_name, "deltaSharing")
        self.assertEqual(spark.read.loaded_path, "profile.json#share.schema.customer")
        self.assertIsNotNone(dataframe.predicate)

    def test_snapshot_reader_reads_the_configured_source(self) -> None:
        dataframe = _DataFrame()
        connector = _Connector(dataframe)
        request = SourceReadRequest.for_snapshot(
            SourceReadPlan(
                source_object="crm.customer",
                capture_mode=SourceCaptureMode.FULL,
            ),
            source_version="42",
        )

        result = SnapshotSourceReader(connector).read(request)

        self.assertIs(result.dataframe, dataframe)
        self.assertEqual(connector.table_name, "crm.customer")
        self.assertEqual(result.evidence.boundary, SnapshotReadBoundary(source_version="42"))
        self.assertIsNone(result.evidence.source_row_count)

    def test_delta_sharing_connector_proves_access_with_one_source_row(self) -> None:
        dataframe = _DataFrame()
        connector = DeltaSharingConnector(
            _SparkSession(dataframe),
            "profile.json",
            health_check_resource="share.schema.customer",
        )

        result = connector.health()

        self.assertEqual(result.connector_type, "DeltaSharingConnector")
        self.assertEqual(result.status, ConnectionHealthStatus.HEALTHY)
        self.assertEqual(result.resource_ref, "share.schema.customer")
        self.assertEqual(dataframe.limit_value, 1)
        self.assertTrue(dataframe.count_called)

    def test_delta_sharing_health_reports_when_a_resource_is_not_configured(self) -> None:
        connector = DeltaSharingConnector(_SparkSession(_DataFrame()), "profile.json")

        health = connector.health()

        self.assertEqual(health.status, ConnectionHealthStatus.NOT_CONFIGURED)

    def test_watermark_reader_uses_an_inclusive_exclusive_predicate(self) -> None:
        dataframe = _DataFrame()
        request = SourceReadRequest.for_watermark(
            SourceReadPlan(
                source_object="crm.customer",
                capture_mode=SourceCaptureMode.WATERMARK,
                watermark_column="modified_at",
            ),
            lower_bound="2026-09-24T00:00:00Z",
            upper_bound="2026-09-25T00:00:00Z",
        )

        result = _WatermarkReader(_Connector(dataframe)).read(request)

        self.assertIs(result.dataframe, dataframe)
        self.assertEqual(
            dataframe.predicate.value if dataframe.predicate else None,
            "(modified_at >= 2026-09-24T00:00:00Z) AND (modified_at < 2026-09-25T00:00:00Z)",
        )
        self.assertEqual(
            result.evidence.boundary,
            WatermarkReadBoundary(
                lower_bound="2026-09-24T00:00:00Z",
                upper_bound="2026-09-25T00:00:00Z",
            ),
        )

    def test_request_rejects_a_boundary_for_the_wrong_capture_mode(self) -> None:
        plan = SourceReadPlan(
            source_object="crm.customer",
            capture_mode=SourceCaptureMode.FULL,
        )

        with self.assertRaisesRegex(TypeError, "FULL read requires"):
            SourceReadRequest(
                plan=plan,
                boundary=WatermarkReadBoundary(lower_bound=1, upper_bound=2),
            )

    def test_snapshot_reader_rejects_a_watermark_request(self) -> None:
        request = SourceReadRequest.for_watermark(
            SourceReadPlan(
                source_object="crm.customer",
                capture_mode=SourceCaptureMode.WATERMARK,
                watermark_column="modified_at",
            ),
            lower_bound=1,
            upper_bound=2,
        )

        with self.assertRaisesRegex(NotImplementedError, "does not support WATERMARK"):
            SnapshotSourceReader(_Connector(_DataFrame())).read(request)