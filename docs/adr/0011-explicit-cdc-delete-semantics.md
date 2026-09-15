# ADR 0011: CDC delete semantics are explicit per dataset

- Status: Accepted
- Date: 2026-09-15

## Context

A CDC delete can mean physical erasure, logical deactivation, SCD2 interval closure, an event to
retain without target mutation, or an operation the target is not allowed to accept. Load-strategy
names do not determine that business meaning.

## Decision

Any capture contract capable of emitting delete operations must explicitly select one compatible
dataset policy:

- `HARD_DELETE`;
- `SOFT_DELETE`;
- `SCD2_CLOSE`;
- `IGNORE`; or
- `REJECT`.

The framework never infers delete behavior from CDC, UPSERT, SCD1 or SCD2. Missing or incompatible
policy fails configuration validation or plan compilation before Spark target execution.

The Spark executor validates source operation codes, applies the selected policy and emits bounded
delete/reject counts plus detailed Delta evidence references. Delete rows and affected keys remain
inside Spark.

## Consequences

- Delete behavior is reviewable as part of the Silver dataset contract.
- `IGNORE` is an explicit business choice, not accidental data loss.
- `REJECT` is available for immutable or unsupported targets.
- Tests must cover each policy only through compatible registered Spark runtimes.
