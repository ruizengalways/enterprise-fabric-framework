from __future__ import annotations

import sys
import types
import unittest
from unittest.mock import patch

from examples.ingest_delta_sharing_execution_group_to_bronze import (
    runtime_factory_from_environment,
)
from enterprise_fabric_framework.orchestration import TableDelivery, TableIngestionMethod
from enterprise_fabric_framework.producer.delta_sharing_job import DeltaSharingTableRuntimeFactory


class _RuntimeFactory(DeltaSharingTableRuntimeFactory):
    def create_ingestion_method(self, delivery: TableDelivery) -> TableIngestionMethod:
        raise NotImplementedError

    def create_data_quality_evaluator(self, delivery: TableDelivery) -> None:
        return None

    def create_reconciliation_evaluator(self, delivery: TableDelivery) -> None:
        return None


class DeltaSharingExecutionGroupExampleTests(unittest.TestCase):
    def test_runtime_factory_uses_the_configured_project_builder(self) -> None:
        module_name = "test_project.delta_sharing_runtime"
        module = types.ModuleType(module_name)
        spark = object()
        captured: dict[str, object] = {}
        factory = _RuntimeFactory(spark, "profile.json")

        def build_factory(
            received_spark: object,
            profile: str,
            environment: dict[str, str],
        ) -> DeltaSharingTableRuntimeFactory:
            captured["spark"] = received_spark
            captured["profile"] = profile
            captured["environment"] = environment
            return factory

        module.build_factory = build_factory  # type: ignore[attr-defined]
        environment = {
            "DELTA_SHARING_PROFILE": "profile.json",
            "DELTA_SHARING_RUNTIME_FACTORY_BUILDER": f"{module_name}:build_factory",
        }

        with patch.dict(sys.modules, {module_name: module}):
            result = runtime_factory_from_environment(spark, environment)

        self.assertIs(result, factory)
        self.assertIs(captured["spark"], spark)
        self.assertEqual(captured["profile"], "profile.json")
        self.assertIs(captured["environment"], environment)

    def test_runtime_factory_builder_requires_module_and_function(self) -> None:
        with self.assertRaisesRegex(ValueError, "package.module:function"):
            runtime_factory_from_environment(
                object(),
                {
                    "DELTA_SHARING_PROFILE": "profile.json",
                    "DELTA_SHARING_RUNTIME_FACTORY_BUILDER": "not-a-builder",
                },
            )


if __name__ == "__main__":
    unittest.main()