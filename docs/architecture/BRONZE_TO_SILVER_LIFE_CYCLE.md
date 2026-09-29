---
id: architecture.silver-lifecycle
status: current
source_of_truth_for:
  - structured-streaming-consumption
  - silver-load-strategies
  - silver-schema-versioning
  - silver-contract-migration
  - replace-publication
last_reviewed: 2026-09-22
---

# Silver lifecycle



## Silver load strategies

| Consumer need | Strategy | Required evidence |
|---|---|---|
| Immutable events/audit | APPEND | Stable event identity and conflict policy |
| Complete compatible current relation | REPLACE | Complete candidate and publication guards |
| Current state with explicit actions | UPSERT | Entity key and delete semantics |
| Current state with overwrite-on-change | SCD1 | Entity key and deterministic winner |
| Point-in-time entity history | SCD2 | Effective ordering and sufficient history fidelity |
| Changes inferred between complete snapshots | SNAPSHOT_DIFF | Two complete comparable snapshots |
| Current relation derived from history | CURRENT_PROJECTION | Authoritative ordered history |

APPEND uses identity validation, conflict detection and insert-only Delta merge. UPSERT and SCD1
may share a current-state kernel but retain distinct public action/evidence contracts. SCD2 performs
ordered affected-key processing and proves one-current-row and non-overlapping interval invariants.
SNAPSHOT_DIFF uses distributed hash/full-outer-join operations. CURRENT_PROJECTION uses
authoritative history at an explicit boundary and MUST NOT collect affected keys to Python.

Append-only Bronze does not imply Silver APPEND. Bronze retains source evidence; Silver strategy
describes consumer-facing mutation.

## Consumer identity, ordering, and checkpoints

- Entity key, event identity and source cursor are distinct roles even when mapped to one column.
- Timestamp alone is not deterministic when ties are possible. A stable sequence, log position or
  event identity MUST break ties, or an explicit conflict policy MUST apply.
- Silver progress and query state follow the Spark checkpoint ownership contract in
  [Source-to-Bronze configuration](CONTROL_PLANE.md#source-to-bronze-configuration).

## Deletes, late data, and Silver schema

Delete meaning is explicit per dataset: `HARD_DELETE`, `SOFT_DELETE`, `SCD2_CLOSE`, `IGNORE` or
`REJECT`. Watermark polling MUST NOT infer a hard delete. Valid evidence includes a source delete
operation, tombstone, soft-delete marker or difference between complete snapshots.

SCD2 late-arrival policy is either:

- `REJECT_AND_REBUILD`; or
- `CORRECT_WITHIN_WINDOW` with authoritative effective ordering and a governed bound.

Unrestricted retroactive correction is unsupported. One-current-row and non-overlapping interval
invariants always apply.

Silver schema policy is always `STRICT`. Any Silver schema change, including an additive nullable
column, requires a new Silver contract version, target relation and checkpoint. The new version is
validated before consumers migrate; executors MUST NOT evolve an existing Silver version in place.
Breaking Bronze grain, key, type meaning or semantics require a new Source-to-Bronze contract
version or explicit quarantine/failure. Bronze schema rules are defined in
[Source-to-Bronze lifecycle](SOURCE_TO_BRONZE_LIFE_CYCLE.md#bronze-schema-lifecycle).

## Common enterprise mappings

| Scenario | Bronze | Typical Silver |
|---|---|---|
| Nightly complete ERP extract | immutable `SNAPSHOT` | REPLACE, or SNAPSHOT_DIFF to SCD1/SCD2 |
| CRM modified timestamp | append-retained observations | UPSERT/SCD1; deletes need separate evidence |
| Ordered database CDC | `EVENT_LOG` | UPSERT/SCD1/SCD2 with explicit delete policy |
| Provider net changes | append-retained reduced-fidelity event log | UPSERT/SCD1; never claim missing history |
| Payment/order events | `EVENT_LOG` | APPEND plus optional CURRENT_PROJECTION |
| Governed full file delivery | `SNAPSHOT` | REPLACE or snapshot diff after completeness proof |
| Master/reference publication | source-faithful capture | validated REPLACE |

## Routine REPLACE

Routine REPLACE keeps the same compatible Silver contract. It MUST:

1. build a run-scoped physical candidate;
2. validate schema, quality, completeness and reconciliation in Spark;
3. publish to the stable Silver relation only after validation;
4. preserve the old stable target after pre-publication failure; and
5. record candidate, publication and target commit evidence.

Routine refresh does not create `v2`.

Publication uses only a Fabric/Delta mechanism that has passed capability and concurrency UAT. If
the platform cannot prove the required stable-target cutover semantics, the plan MUST fail rather
than silently fall back to unsafe in-place overwrite. Failed candidates are retained for a bounded
diagnostic TTL and removed by a separate idempotent cleanup process.

## Breaking Silver contract migration

A new Silver contract version is required for every Silver schema change and for any change to
grain, keys, delete/history semantics, source fidelity or transformation meaning. This includes
an otherwise compatible nullable-column addition: Silver v1 remains immutable and Silver v2 is
developed and validated independently.

When Bronze remains valid:

```text
shared Bronze -> Silver v1 -> existing consumers
              -> Silver v2 -> validation -> migrated consumers
```

When Bronze is also wrong:

```text
source -> Bronze v1 -> Silver v1 -> existing consumers
      -> Bronze v2 -> Silver v2 -> validation -> migrated consumers
```

Each Silver version has its own target and Structured Streaming checkpoint and independently
consumes the shared append-only Bronze relation. The framework MUST NOT require matching progress,
paired runs or capture fan-out coordination. v2 may legitimately differ from v1 because it corrects
the contract. Operators MAY choose an explicit cutoff date for comparison, declaring its column,
timezone and inclusivity; the framework does not synchronize the versions. v2 is validated against
source facts and invariants, not assumed-correct v1 output. Domain repositories own Gold/report
cutover.

When Bronze is also wrong, the domain engineer copies the source-table capture configuration,
corrects it and registers a new Source-to-Bronze configuration with a new Bronze relation and
independent producer progress. Silver v2 references that new relation. This is an independent
ingestion chain; the framework does not repair or coordinate Bronze v1/v2 in place. Historical
recapture remains limited by what the source still exposes.

Lifecycle MAY expose `BUILDING`, `READY_FOR_CONSUMER_VALIDATION`, `ACTIVE`, `DEPRECATED` and
`RETIRED`, but a successful Spark run alone MUST NOT mark v2 active without required domain
acceptance.

v1 Silver processing may stop after registered consumers migrate or accept retirement. Its
checkpoint MAY be deleted as part of permanent decommissioning under the lifecycle rules in
[Source-to-Bronze configuration](CONTROL_PLANE.md#source-to-bronze-configuration). Shared Bronze
ingestion remains active while another consumer needs it. Data deletion is a separate cleanup
operation. The framework SHOULD retire v1 rather than overwrite it with v2 semantics.

A shallow clone MAY accelerate short-lived validation, but its source files remain dependencies;
source VACUUM is prohibited until the clone is removed or made independent.

## Pattern selection workflow

For every dataset:

1. complete the source fact sheet;
2. select capture semantics and one truthful Bronze representation;
3. resolve a compatible source reader and writer when Spark capture is used;
4. select Silver semantics from consumer needs, not source technology;
5. declare identity, ordering, delete, late-data, schema, retention and replay policies;
6. compile only registered compatible capabilities;
7. prove the combination with local Spark/Delta and required Fabric UAT; and
8. preserve fidelity limitations in metadata and run evidence.