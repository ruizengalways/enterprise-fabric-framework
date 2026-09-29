from __future__ import annotations

import unittest

from examples.ingest_delta_sharing_direct_to_bronze import (
    IngestionSettings,
    build_request,
    ingest,
    settings_from_environment,
)
from enterprise_fabric_framework.producer.readers import (
    SnapshotReadBoundary,
    SourceCaptureMode,
    WatermarkReadBoundary,
)


class _DataFrameWriter:
    def __init__(self) -> None:
        self.format_name: str | None = None
        self.write_mode: str | None = None
        self.table_name: str | None = None

    def format(self, format_name: str) -> _DataFrameWriter:
        self.format_name = format_name
        return self

    def mode(self, write_mode: str) -> _DataFrameWriter:
        self.write_mode = write_mode
        return self

    def saveAsTable(self, table_name: str) -> None:
        self.table_name = table_name


class _DataFrame:
    def __init__(self) -> None:
        self.write = _DataFrameWriter()


class _SparkReader:
    def __init__(self, dataframe: _DataFrame) -> None:
        self._dataframe = dataframe

    def format(self, _: str) -> _SparkReader:
        return self

    def load(self, _: str) -> _DataFrame:
        return self._dataframe


class _SparkSession:
    def __init__(self, dataframe: _DataFrame) -> None:
        self.read = _SparkReader(dataframe)


class IngestDeltaSharingToBronzeTests(unittest.TestCase):
    def test_full_settings_build_a_snapshot_request(self) -> None:
        request = build_request(
            settings_from_environment(
                {
                    "DELTA_SHARING_PROFILE": "profile.json",
                    "DELTA_SHARING_TABLE": "share.crm.customer",
                    "BRONZE_TABLE": "bronze.customer",
                    "CAPTURE_MODE": "full",
                    "SOURCE_VERSION": "17",
                }
            )
        )

        self.assertEqual(request.plan.capture_mode, SourceCaptureMode.FULL)
        self.assertEqual(request.boundary, SnapshotReadBoundary(source_version="17"))

    def test_watermark_settings_require_a_complete_interval(self) -> None:
        with self.assertRaisesRegex(ValueError, "UPPER_BOUND must be set"):
            settings_from_environment(
                {
                    "DELTA_SHARING_PROFILE": "profile.json",
                    "DELTA_SHARING_TABLE": "share.crm.customer",
                    "BRONZE_TABLE": "bronze.customer",
                    "CAPTURE_MODE": "watermark",
                    "WATERMARK_COLUMN": "modified_at",
                    "LOWER_BOUND": "2026-09-24T00:00:00Z",
                }
            )

    def test_ingest_appends_the_reader_result_to_the_bronze_table(self) -> None:
        dataframe = _DataFrame()
        evidence = ingest(
            _SparkSession(dataframe),
            IngestionSettings(
                profile="profile.json",
                source_object="share.crm.customer",
                bronze_table="bronze.customer",
                capture_mode=SourceCaptureMode.FULL,
            ),
        )

        self.assertEqual(dataframe.write.format_name, "delta")
        self.assertEqual(dataframe.write.write_mode, "append")
        self.assertEqual(dataframe.write.table_name, "bronze.customer")
        self.assertEqual(evidence.capture_mode, SourceCaptureMode.FULL)
        self.assertEqual(evidence.boundary, SnapshotReadBoundary())

    def test_watermark_request_carries_the_selected_interval(self) -> None:
        request = build_request(
            IngestionSettings(
                profile="profile.json",
                source_object="share.crm.customer",
                bronze_table="bronze.customer",
                capture_mode=SourceCaptureMode.WATERMARK,
                watermark_column="modified_at",
                lower_bound="2026-09-24T00:00:00Z",
                upper_bound="2026-09-25T00:00:00Z",
            )
        )

        self.assertEqual(
            request.boundary,
            WatermarkReadBoundary(
                lower_bound="2026-09-24T00:00:00Z",
                upper_bound="2026-09-25T00:00:00Z",
            ),
        )