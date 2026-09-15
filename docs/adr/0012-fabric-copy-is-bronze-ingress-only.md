# ADR 0012: Fabric Copy is permitted as Bronze ingress only

- Status: Accepted
- Date: 2026-09-15

## Context

Fabric Copy can provide efficient, managed connectivity for enterprise databases, SaaS systems and
files. Reimplementing every connector in Spark would waste a useful Fabric-native capability.
Allowing Copy to implement curated business semantics, however, would create a second execution
engine beside the Spark runtime.

## Decision

Fabric Copy may transport and land source-faithful data in Bronze. Its handoff to Spark must provide
a bounded capture manifest containing the run identity, landed relation/file references, source
boundary or provider token, completion status and available source/copy counts.

Spark exclusively owns:

- normalization and canonical business-row hashing;
- deduplication and deterministic ordering;
- schema and data-quality decisions;
- reconciliation;
- snapshot diff, CDC interpretation and current projection; and
- every Silver target mutation.

Copy must not write a Silver target or implement APPEND, REPLACE, UPSERT, SCD1 or SCD2 semantics.
Spark execution begins only after the Bronze handoff is complete and validated. Framework
checkpoints advance only after the corresponding Silver commit and required reconciliation pass.

## Consequences

- The framework remains Fabric-native without maintaining duplicate business logic.
- Copy-specific orchestration belongs behind `platform/fabric` and bounded contracts.
- Copy output is not trusted as a curated result merely because the activity succeeded.
- Fabric UAT must cover Copy-to-Bronze handoff, partial/failing Copy runs and replay.
