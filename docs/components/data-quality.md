# Data quality

## Purpose

Data-quality evaluators assess one Spark DataFrame before a layer publishes or mutates its target.
They return only Spark DataFrames and bounded evidence; they never collect business rows to Python
or make a publication decision themselves. The same evaluator contract applies to
Source-to-Bronze and Bronze-to-Silver work.

## Contracts

`DataQualityEvaluator` is the extension point for complex source- or domain-specific checks. Its
`evaluate(dataframe, run_id=...)` method returns `QualityEvaluation`:

- `valid_dataframe`: rows eligible for the next layer operation;
- `quarantine_dataframe`: failed rows retained for a separately configured quarantine sink; and
- `result`: `QualityRunResult`, including bounded per-rule outcomes and aggregate evaluated,
  accepted, and quarantined row counts.

`SparkSqlDataQualityEvaluator` is the standard implementation for rules that can be expressed as
Spark SQL predicates. Each `SparkSqlQualityRule` identifies a rule, a predicate that must evaluate
to `true`, a `QualityFailureAction`, and a severity:

| Action | Valid dataframe | Run status |
|---|---|---|
| `REPORT` | retains failed rows | warning when rows fail |
| `QUARANTINE` | removes failed rows | warning when rows fail |
| `FAIL` | removes failed rows | fails |

`FAIL` requires `CRITICAL` severity and produces a failed table delivery. `WARNING` rules use
`REPORT` or supported `QUARANTINE` behavior; they record a warning but do not block another table
in the parent execution group. Null predicate results are failures. A caller must not publish a
failed evaluation. A caller that
receives quarantined rows must write them to a configured quarantine sink before it can regard the
delivery as successful; the controlled Bronze example deliberately fails instead of silently
discarding them.

## Custom evaluation

Subclass `DataQualityEvaluator` when a rule needs joins, reference data, schema-aware checks, or a
source-specific Spark algorithm. Implementations perform their transformations and aggregates in
Spark/Delta, then return a `QualityEvaluation` with bounded `QualityRunResult` evidence. They can
be injected into either producer or consumer runtime without a layer-specific subclass.
