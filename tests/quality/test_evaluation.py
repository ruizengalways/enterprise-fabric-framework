from __future__ import annotations

import unittest

from enterprise_fabric_framework.quality import (
    DataQualityEvaluator,
    DataQualitySeverity,
    DataQualityStatus,
    QualityFailureAction,
    QualityEvaluation,
    QualityRunResult,
    SparkSqlDataQualityEvaluator,
    SparkSqlQualityRule,
)
from enterprise_fabric_framework.reconciliation import ReconciliationSeverity


class _CustomEvaluator(DataQualityEvaluator):
    def evaluate(self, dataframe: object, *, run_id: str) -> QualityEvaluation:
        return QualityEvaluation(
            valid_dataframe=dataframe,
            quarantine_dataframe=None,
            result=QualityRunResult(run_id=run_id, overall_status="PASSED", rule_results=()),
        )


class DataQualityEvaluationTests(unittest.TestCase):
    def test_custom_evaluator_can_supply_layer_specific_behavior(self) -> None:
        dataframe = object()

        evaluation = _CustomEvaluator().evaluate(dataframe, run_id="bronze-42")

        self.assertIs(evaluation.valid_dataframe, dataframe)
        self.assertEqual(evaluation.result.overall_status, "PASSED")

    def test_spark_sql_evaluator_rejects_duplicate_rule_ids(self) -> None:
        rule = SparkSqlQualityRule("customer-id", "customer_id IS NOT NULL")

        with self.assertRaisesRegex(ValueError, "unique"):
            SparkSqlDataQualityEvaluator((rule, rule))

    def test_quality_run_result_rejects_negative_bounded_counts(self) -> None:
        with self.assertRaisesRegex(ValueError, "accepted_rows"):
            QualityRunResult(
                run_id="bronze-42",
                overall_status="PASSED",
                rule_results=(),
                accepted_rows=-1,
            )

    def test_warning_report_rule_does_not_block_the_delivery(self) -> None:
        rule = SparkSqlQualityRule(
            "optional-reference-check",
            "reference_found",
            failure_action=QualityFailureAction.REPORT,
            severity=DataQualitySeverity.WARNING,
        )

        self.assertEqual(rule.severity, DataQualitySeverity.WARNING)
        self.assertIsNot(DataQualitySeverity, ReconciliationSeverity)

    def test_warning_rule_cannot_use_a_blocking_failure_action(self) -> None:
        with self.assertRaisesRegex(ValueError, "CRITICAL"):
            SparkSqlQualityRule(
                "optional-reference-check",
                "reference_found",
                severity=DataQualitySeverity.WARNING,
            )

    def test_quality_run_exposes_warning_and_critical_statuses(self) -> None:
        warning = QualityRunResult(
            run_id="bronze-42",
            overall_status=DataQualityStatus.WARNING,
            rule_results=(),
        )
        failed = QualityRunResult(
            run_id="bronze-42",
            overall_status=DataQualityStatus.FAILED,
            rule_results=(),
        )

        self.assertTrue(warning.has_warnings)
        self.assertFalse(warning.has_critical_failure)
        self.assertTrue(failed.has_critical_failure)


if __name__ == "__main__":
    unittest.main()