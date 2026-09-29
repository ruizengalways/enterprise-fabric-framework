from __future__ import annotations

import unittest

from enterprise_fabric_framework.reconciliation import (
    CompositeReconciliationEvaluator,
    ReconciliationEvidence,
    ReconciliationFailureAction,
    ReconciliationRule,
    ReconciliationRuleResult,
    ReconciliationSeverity,
    ReconciliationStatus,
    RowCountReconciliationEvaluator,
)


class _FailingRule(ReconciliationRule):
    def evaluate(self, evidence: ReconciliationEvidence) -> ReconciliationRuleResult:
        return ReconciliationRuleResult(
            rule_id=self.rule_id,
            passed=False,
            detail="custom rule failed",
            severity=self.severity,
        )


class RowCountReconciliationEvaluatorTests(unittest.TestCase):
    def test_passes_when_published_and_rejected_rows_account_for_input(self) -> None:
        result = RowCountReconciliationEvaluator().evaluate(
            ReconciliationEvidence(input_rows=12, output_rows=10, rejected_rows=2),
            run_id="bronze-42",
        )

        self.assertTrue(result.passed)
        self.assertEqual(result.rule_results[0].expected_rows, 12)
        self.assertEqual(result.rule_results[0].actual_rows, 12)

    def test_fails_when_required_row_count_evidence_is_missing(self) -> None:
        result = RowCountReconciliationEvaluator().evaluate(
            ReconciliationEvidence(input_rows=None, output_rows=12),
            run_id="silver-42",
        )

        self.assertFalse(result.passed)
        self.assertIn("required", result.rule_results[0].detail or "")

    def test_fails_when_row_counts_do_not_balance(self) -> None:
        result = RowCountReconciliationEvaluator().evaluate(
            ReconciliationEvidence(input_rows=12, output_rows=10, rejected_rows=1),
            run_id="bronze-42",
        )

        self.assertFalse(result.passed)
        self.assertEqual(result.rule_results[0].detail, "row-count balance mismatch")

    def test_warning_rules_are_recorded_without_blocking_the_delivery(self) -> None:
        result = CompositeReconciliationEvaluator(
            (
                _FailingRule(
                    "optional-check",
                    failure_action=ReconciliationFailureAction.REPORT,
                    severity=ReconciliationSeverity.WARNING,
                ),
                _FailingRule(
                    "another-optional-check",
                    failure_action=ReconciliationFailureAction.REPORT,
                    severity=ReconciliationSeverity.WARNING,
                ),
            )
        ).evaluate(
            ReconciliationEvidence(input_rows=12, output_rows=12),
            run_id="bronze-42",
        )

        self.assertTrue(result.passed)
        self.assertTrue(result.has_warnings)
        self.assertEqual(result.status, ReconciliationStatus.WARNING)
        self.assertEqual(len(result.rule_results), 2)

    def test_critical_rule_blocks_the_delivery_after_all_rules_run(self) -> None:
        result = CompositeReconciliationEvaluator(
            (
                _FailingRule("critical-check"),
                _FailingRule(
                    "optional-check",
                    failure_action=ReconciliationFailureAction.REPORT,
                    severity=ReconciliationSeverity.WARNING,
                ),
            )
        ).evaluate(
            ReconciliationEvidence(input_rows=12, output_rows=12),
            run_id="bronze-42",
        )

        self.assertFalse(result.passed)
        self.assertEqual(result.status, ReconciliationStatus.FAILED)
        self.assertEqual(len(result.rule_results), 2)

    def test_warning_rule_cannot_use_a_blocking_failure_action(self) -> None:
        with self.assertRaisesRegex(ValueError, "CRITICAL"):
            _FailingRule("optional-check", severity=ReconciliationSeverity.WARNING)


if __name__ == "__main__":
    unittest.main()