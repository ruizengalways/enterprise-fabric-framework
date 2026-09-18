---
id: architecture.data-lifecycle
status: current
source_of_truth_for:
  - source-facts
  - structured-streaming-execution
  - capture-semantics
  - bronze-representations
  - silver-load-strategies
  - silver-versioning
last_reviewed: 2026-09-17
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

## Structured Streaming execution

Bronze-to-Silver processing MUST use Spark Structured Streaming in micro-batch mode through the
public runtime. Scheduled jobs SHOULD use `AvailableNow` to drain the input available at query
start and then exit with a source certified for that trigger; a continuously running query MAY use
a processing-time trigger. Pipeline scheduling controls when a job starts, while Spark manages that
query's offsets and processing state.

Load and reconciliation executors run over Spark micro-batch DataFrames in `foreachBatch` where
needed. A callback MUST complete required target writes, reconciliation and durable batch evidence
before returning successfully. Writes MUST be replay-safe when the callback fails after a commit;
Spark checkpoints alone do not make arbitrary `foreachBatch` side effects exactly-once.

Spark-native ingress SHOULD use a supported streaming reader. Fabric Copy/Dataflow and protocols
without a streaming reader MAY use bounded extraction to publish immutable Bronze deliveries;
they do not require a custom streaming source merely to imitate this execution model. Downstream
Silver processing uses the same Structured Streaming runtime regardless of ingress provider.

A micro-batch is an execution unit, not necessarily a complete source delivery or snapshot.
Snapshot-dependent strategies MUST assemble and validate a named complete snapshot before REPLACE
or SNAPSHOT_DIFF; batch splitting MUST NOT cause partial publication or false inferred deletes.
Empty snapshots also require explicit completion evidence. Publication metadata is local to the
Bronze producer and does not coordinate Silver versions.

Verified on 2026-09-17 against the Spark 4.1 documentation:
[triggers, foreachBatch and checkpoint recovery](https://spark.apache.org/docs/4.1.0/streaming/apis-on-dataframes-and-datasets.html).

## Bounded Source-to-Bronze extraction

Bounded extraction means one producer invocation reads a finite, declared source selection and
then ends. It does not require `readStream` support at the source. Examples include:

- Copy reads one declared full extract or rows in `(previous_watermark, upper_watermark]`.
- An API reader traverses all pages for a declared window until the provider's end-of-results
  condition is reached.

The producer MUST declare its selection, termination condition and completeness evidence. A
source upper boundary SHOULD be frozen before extraction where supported. A completed pagination
loop does not by itself prove a point-in-time snapshot of a changing source; evidence MUST preserve
the source's actual fidelity. Retry/publication identities follow the Bronze representation
contract, and source cursor ownership follows `CONTROL_PLANE.md#operational-state`.

Both bounded and streaming producers publish append-only Bronze. The Silver consumer reads the
configured Bronze Delta relation through Structured Streaming with its stable checkpoint. A
bounded producer is not a separate batch Silver runtime and MUST NOT require consumers to select
one capture ID per invocation. Source extraction progress is independent of Silver consumption
progress; retrying Silver does not rerun source extraction.

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
| Watermark | Rows matching a monotonic predicate | Append-retained observations in `EVENT_LOG` | Hard deletes normally remain invisible |
| Watermark with lookback | Re-observation of a recent window | Append-retained observations in `EVENT_LOG` | Lookback does not create delete visibility |
| Net-change CDC | Final supplied change per entity/window | Reduced-fidelity `EVENT_LOG` | Upstream may discard intermediate states |
| All-change CDC | Ordered insert/update/delete changes | `EVENT_LOG` | Requires stable position and operation |
| Business events/audit | Distinct immutable events | `EVENT_LOG` | Requires stable event identity |
| File drop | Facts declared by the file contract | Snapshot or event log with manifest | Arrival does not prove completeness |

CDC is capture semantics, not a Silver load strategy.

## Bronze representations

Every Source-to-Bronze configuration MUST select exactly one authoritative representation. All
authoritative Bronze writes MUST be append-only: existing observations MUST NOT be updated,
deleted or replaced.
All successfully published source deliveries remain retained for replay. There is no rolling
current-stage Bronze or routine business-data expiry in this model; retirement cleanup is separate.
Compaction MAY change physical layout while preserving logical rows and supported reader recovery.

| Representation | Writer behavior | Required contract |
|---|---|---|
| `EVENT_LOG` | Identity-aware insert-only Delta write of events, changes or observations | Declared record identity, source fidelity, ordering and conflict policy |
| `SNAPSHOT` | Append one complete immutable capture under a `capture_id` | Completeness evidence, schema identity and retention |
| `EPHEMERAL` | Reserved non-durable run-scoped handoff; unavailable in the first release | Future explicit approved exception |

An exact `EVENT_LOG` replay is a no-op. Reuse of an identity with different canonical content MUST
fail and persist conflict evidence. A row hash MAY be identity only when the source contract says
identical canonical observations are the same record; it is never an automatic fallback.
Hash algorithm, canonicalization version and participating business columns MUST be frozen in the
plan/evidence; ingestion metadata is excluded unless explicitly part of source identity.

Watermark and net-change observations MUST retain their declared source window and fidelity. An
observation identity MAY identify a stable delivery plus a row within that delivery; it does not
prove a source event or an intermediate change. Overlapping deliveries MAY retain repeated source
observations. Silver policies resolve them deterministically without discarding Bronze history.

Insert-only replay MUST suppress an exact retry of the same declared record identity and reject
conflicting content. Retry identity MUST remain stable across producer restarts. For snapshots,
`capture_id` identifies a complete snapshot and its publication evidence; it is not a mandatory
launcher input for Silver consumers. A snapshot MUST NOT become eligible before its data and
completeness evidence are durably published.

Retaining logical Bronze rows does not retain every obsolete Delta file or log version forever.
Cleanup MUST preserve files/logs required by active or resumable streaming readers and approved
replay requests. An unreadable checkpoint boundary MUST fail closed; it MUST NOT silently skip
ahead. A full rebuild can use the retained current Bronze relation with a fresh checkpoint.

`EPHEMERAL` remains a representation placeholder. The first release MUST NOT register an
`EPHEMERAL` writer or accept it as an executable capture plan. Its implementation will be
reconsidered after the durable insert-only Bronze (`EVENT_LOG`) writer is complete.

If consumers need immutable history and convenient current state, the immutable Bronze remains
authoritative and a registered projection produces the current relation. Two independently
authoritative histories require separate Source-to-Bronze configurations and explicit lineage.

## Spark-native source and writer separation

A source reader obtains a Spark DataFrame and bounded source evidence. A separately registered
writer appends the selected Bronze representation and publishes bounded delivery evidence.

```text
spark/capture/
  sources/       Delta Sharing, API, JDBC or file protocol readers
  writers/       append-only EVENT_LOG and SNAPSHOT writers
```

The reader MUST NOT select Bronze mutation or Silver strategy. The writer MUST NOT contain
connector or Silver logic. Fabric Copy and Dataflow Gen2 do not use these Python writers, but their
output and publication evidence MUST satisfy the same representation contract.

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

## Identity, ordering and checkpoint invariants

- Entity key, event identity and source cursor are distinct roles even when mapped to one column.
- Timestamp alone is not deterministic when ties are possible. A stable sequence, log position or
  event identity MUST break ties, or an explicit conflict policy MUST apply.
- A source upper boundary SHOULD be frozen before extraction where supported.
- Baseline-to-incremental handoff MUST prove no gap through source position, overlap/replay or an
  equivalent source-supported boundary.
- A source cursor or query bootstrap MUST NOT skip to “now” without handoff evidence.
- Silver progress and query state follow the Spark checkpoint ownership contract in
  [Source-to-Bronze configuration](CONTROL_PLANE.md#source-to-bronze-configuration).

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

Each Silver version has its own target and Structured Streaming checkpoint and independently
consumes the shared append-only Bronze relation. The framework MUST NOT require matching progress,
paired runs or capture fan-out coordination. v2 may legitimately differ from v1 because it corrects
the contract. Operators MAY choose an explicit cutoff date for comparison, declaring its column,
timezone and inclusivity; the framework does not synchronize the versions. v2 is validated against
source facts and invariants, not assumed-correct v1 output. Domain repositories own Gold/report
cutover.

When Bronze is also wrong, the domain engineer copies the source-table capture configuration,
corrects it and registers a new Source-to-Bronze configuration with a new Bronze relation and
independent producer progress. Silver v2 references that new relation. This is an independent ingestion chain;
the framework does not repair or coordinate Bronze v1/v2 in place. Historical recapture remains
limited by what the source still exposes.

Lifecycle MAY expose `BUILDING`, `READY_FOR_CONSUMER_VALIDATION`, `ACTIVE`, `DEPRECATED` and
`RETIRED`, but a successful Spark run alone MUST NOT mark v2 active without required domain
acceptance.

v1 Silver processing may stop after registered consumers migrate or accept retirement. Its
checkpoint MAY be deleted as part of permanent decommissioning under the lifecycle rules in
[Source-to-Bronze configuration](CONTROL_PLANE.md#source-to-bronze-configuration). Shared Bronze ingestion remains active while
another consumer needs it. Data deletion is a separate cleanup operation. The framework SHOULD
retire v1 rather than overwrite it with v2 semantics.

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
