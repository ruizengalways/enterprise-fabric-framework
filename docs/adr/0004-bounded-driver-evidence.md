# ADR 0004: Bounded driver evidence

- Status: Proposed
- Date: 2026-09-15

## Decision

Production code may collect only bounded scalar aggregates, `LIMIT 1` existence guards and
explicitly capped diagnostic samples. It must not collect a source, stage, target, affected-key
set, quarantine set or partition-wide metric set. `toPandas()` is prohibited.

Detailed reconciliation and quarantine evidence is written as Delta data and referenced by the
bounded run result.

## Consequences

- Runtime result contracts contain counts, statuses, versions and evidence references, not rows.
- Static and runtime guards enforce collection boundaries.
- Driver memory is not proportional to business-table size or partition cardinality.
