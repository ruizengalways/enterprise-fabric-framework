---
id: architecture.data-lifecycle
status: current
source_of_truth_for:
  - source-facts
  - capture-semantics
  - bronze-representations
  - silver-load-strategies
  - silver-versioning
last_reviewed: 2026-09-16
---

# Data lifecycle

## Governing principle

The framework MUST NOT manufacture source history. A target can preserve only the facts the source
actually exposes:

- a full extract proves state at successful snapshot boundaries, not intermediate changes;
- a watermark proves rows returned by its predicate and normally cannot reveal hard deletes;
- a net-change feed proves the final supplied change in a window, not discarded states; and
- an event feed proves individual events only with stable identity and ordering.

Capture mode, Bronze representation and Silver load strategy are independent:

```text
source facts -> capture mode -> Bronze representation -> Silver load strategy
```

## Dataset onboarding fact sheet

Before a strategy is selected, the owner MUST declare:

1. baseline availability and completeness;
2. whether later deliveries are snapshots, watermark observations, net changes, all changes,
   files or business events;
3. entity/business key;
4. event/change identity when applicable;
5. deterministic ordering and timestamp tie breaker;
6. physical delete, soft-delete or tombstone visibility;
7. late/backdated arrival behavior;
8. whether the provider collapses changes;
9. required current, historical or immutable-event outputs; and
10. evidence proving extraction completeness and checkpoint boundaries.

Unknown required facts MUST cause plan compilation to fail closed.

## Capture semantics

| Capture pattern | What the source proves | Normal Bronze representation | Limitation |
|---|---|---|---|
| Complete snapshot | State at a named complete boundary | `SNAPSHOT` | Intermediate changes are unknown |
| Watermark | Rows matching a monotonic predicate | `CURRENT_STAGE` or governed observations | Hard deletes normally remain invisible |
| Watermark with lookback | Re-observation of a recent window | Deduplicated governed observations | Lookback does not create delete visibility |
| Net-change CDC | Final supplied change per entity/window | Reduced-fidelity `EVENT_LOG` or current stage | Upstream may discard intermediate states |
| All-change CDC | Ordered insert/update/delete changes | `EVENT_LOG` | Requires stable position and operation |
| Business events/audit | Distinct immutable events | `EVENT_LOG` | Requires stable event identity |
| File drop | Facts declared by the file contract | Snapshot or event log with manifest | Arrival does not prove completeness |

CDC is capture semantics, not a Silver load strategy.

## Bronze representations

Every dataset contract MUST select exactly one authoritative representation. There is no implicit
default and no generic `bronze_append` flag.

| Representation | Writer behavior | Required contract |
|---|---|---|
| `EVENT_LOG` | Identity-aware insert-only Delta write | Event/change identity, ordering and conflict policy |
| `SNAPSHOT` | Append one complete immutable capture under a `capture_id` | Completeness evidence, schema identity and retention |
| `CURRENT_STAGE` | Validated full replacement or deterministic keyed merge | Entity key, ordering, delete and replay retention |
| `EPHEMERAL` | Non-durable run-scoped handoff | Explicit approved exception |

An exact `EVENT_LOG` replay is a no-op. Reuse of an identity with different canonical content MUST
fail and persist conflict evidence. A row hash MAY be identity only when the source contract says
identical canonical observations are the same event; it is never an automatic fallback.
Hash algorithm, canonicalization version and participating business columns MUST be frozen in the
plan/evidence; ingestion metadata is excluded unless explicitly part of source identity.

`CURRENT_STAGE` claims only current observed state. Its completed manifest MUST pin a readable
Delta version through downstream completion and the permitted retry window. If that version is no
longer readable, replay fails closed rather than reading the newest state.

If consumers need immutable history and convenient current state, the immutable Bronze remains
authoritative and a registered projection produces the current relation. Two independently
authoritative forms require separate versioned dataset contracts and explicit lineage.

## Spark-native source and writer separation

A source reader obtains a Spark DataFrame and bounded source evidence from a frozen boundary. A
separately registered writer persists the selected Bronze representation and capture manifest.

```text
spark/capture/
  sources/       Delta Sharing, API, JDBC or file protocol readers
  writers/       EVENT_LOG, SNAPSHOT, CURRENT_STAGE and approved EPHEMERAL writers
```

The reader MUST NOT select Bronze mutation or Silver strategy. The writer MUST NOT contain
connector or Silver logic. Fabric Copy and Dataflow Gen2 do not use these Python writers, but their
output and manifest MUST satisfy the same representation contract.

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

Bronze `CURRENT_STAGE` is not automatically Silver SCD1 or REPLACE. Bronze describes retained
source evidence; Silver strategy describes consumer-facing mutation.

## Identity, ordering and checkpoint invariants

- Entity key, event identity and source cursor are distinct roles even when mapped to one column.
- Timestamp alone is not deterministic when ties are possible. A stable sequence, log position or
  event identity MUST break ties, or an explicit conflict policy MUST apply.
- A source upper boundary SHOULD be frozen before extraction where supported.
- Baseline-to-incremental handoff MUST prove no gap through source position, overlap/replay or an
  equivalent source-supported boundary.
- A checkpoint MUST NOT be set to “now” without handoff evidence.
- Checkpoint compare-and-swap or lease fencing MUST happen only after Silver and reconciliation.

## Deletes, late data and schema

Delete meaning is explicit per dataset: `HARD_DELETE`, `SOFT_DELETE`, `SCD2_CLOSE`, `IGNORE` or
`REJECT`. Watermark polling MUST NOT infer a hard delete. Valid evidence includes a source delete
operation, tombstone, soft-delete marker or difference between complete snapshots.

SCD2 late-arrival policy is either:

- `REJECT_AND_REBUILD`; or
- `CORRECT_WITHIN_WINDOW` with authoritative effective ordering and a governed bound.

Unrestricted retroactive correction is unsupported. One-current-row and non-overlapping interval
invariants always apply.

Schema policy is `STRICT` by default. `ADDITIVE_NULLABLE` MAY accept reviewed compatible additions.
Breaking grain, key, type meaning or semantics require a new dataset contract version or explicit
quarantine/failure; executors MUST NOT silently evolve them.

## Common enterprise mappings

| Scenario | Bronze | Typical Silver |
|---|---|---|
| Nightly complete ERP extract | immutable `SNAPSHOT` | REPLACE, or SNAPSHOT_DIFF to SCD1/SCD2 |
| CRM modified timestamp | `CURRENT_STAGE` or governed observations | UPSERT/SCD1; deletes need separate evidence |
| Ordered database CDC | `EVENT_LOG` | UPSERT/SCD1/SCD2 with explicit delete policy |
| Provider net changes | reduced-fidelity event log or current stage | UPSERT/SCD1; never claim missing history |
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

A new contract version is required when grain, keys, delete/history semantics, canonical schema,
source fidelity or transformation meaning changes incompatibly.

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

Each Silver version has independent target/checkpoint state. During migration, new captures are
fanned out to both versions where the Bronze contract permits it. v2 is validated against source
facts and invariants, not assumed-correct v1 output. Domain repositories own Gold/report cutover.

Lifecycle MAY expose `BUILDING`, `READY_FOR_CONSUMER_VALIDATION`, `ACTIVE`, `DEPRECATED` and
`RETIRED`, but a successful Spark run alone MUST NOT mark v2 active without required domain
acceptance.

v1 ingestion may stop only after registered consumers migrate or accept retirement. Data deletion
is a separate authorized cleanup operation after the rollback window. The framework SHOULD retire
v1 rather than overwrite it with v2 semantics.

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
