# Reconciliation

## Purpose

Reconciliation verifies bounded evidence for one input-to-output delivery after the layer-owned
Spark/Delta operation has calculated its aggregates. It applies to both Source-to-Bronze and
Bronze-to-Silver: only the source of the input and output evidence changes.

## Contracts

`ReconciliationEvidence` carries input, output, and rejected row counts plus optional immutable
input/output references. It contains no business rows. `ReconciliationEvaluator` is the abstract
extension point; its `evaluate(evidence, run_id=...)` method returns a bounded
`ReconciliationResult` composed of `ReconciliationRuleResult` values. A
`CompositeReconciliationEvaluator` evaluates every configured `ReconciliationRule`, allowing
custom count, hash, aggregate, or domain-specific rules to run together.

`RowCountReconciliationEvaluator` is the standard rule. It proves the accounting invariant:

$$
	ext{input rows} = \text{output rows} + \text{rejected rows}
$$

Missing required counts or an imbalance fail reconciliation. A producer or consumer must not mark
its manifest successful when reconciliation fails.

Each rule declares `CRITICAL` or `WARNING` severity with a `FAIL` or `REPORT` action. `FAIL`
requires `CRITICAL` severity and fails that table delivery. A `WARNING` plus `REPORT` preserves
the bounded failure evidence while allowing the table and later tables in the parent execution
group to continue.

## Custom evaluation

Subclass `ReconciliationEvaluator` for aggregate comparisons that require business semantics, such
as key-level completeness, distributed checksums, financial totals, or target-specific mutation
counts. Compute those aggregates with Spark/Delta, retain detailed evidence externally when
required, and return references and aggregate outcomes only. The same subclass can be injected
into Source-to-Bronze or Bronze-to-Silver runtime composition.
