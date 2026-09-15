# Enterprise data patterns

This document defines the source facts and target semantics the framework must support. It is a
business-pattern catalog, not an alternate runtime design. Every implementation referenced here
must execute through the production Spark/Delta runtime.

The patterns stop at Silver. Target semantics in this document describe reusable Silver Delta
datasets, not Gold star schemas, KPIs, semantic models or reporting views. Each domain repository
owns those downstream choices under ADR 0009.

The catalog was re-specified after reviewing patterns from the previous repository. Its useful
business semantics are retained; legacy APIs, package boundaries and implementation assumptions
are deliberately not carried forward.

## The governing principle: never manufacture source history

A target can preserve only the history the source actually exposes. A framework must not label
repeated observations as change events or claim event-time accuracy that the source cannot prove.

Examples:

- a full extract proves state at each successful snapshot boundary, not every change between two
  extracts;
- a watermark query proves rows returned by its predicate, but normally does not reveal physical
  deletes;
- a provider's net-change feed proves the final change in its window, not discarded intermediate
  states; and
- a business-event feed can prove individual events only when it has stable identity and ordering
  semantics.

This truth ceiling must be visible in metadata, runtime evidence and consumer documentation.

## Source fact sheet required during dataset onboarding

Before selecting APPEND, REPLACE, UPSERT, SCD1 or SCD2, the dataset owner must answer:

1. Is a complete baseline available?
2. Are later deliveries complete snapshots, watermark observations, net changes, all changes,
   file drops or business events?
3. What is the entity/business key?
4. For event-like data, what is the event identity? It is often not the entity key.
5. What gives deterministic ordering when timestamps tie?
6. Can the source expose physical deletes, soft deletes or tombstones?
7. Can records arrive late or be backdated?
8. Can the provider collapse multiple changes into one delivered record?
9. Does the consumer require current state, point-in-time history, immutable events or more than
   one of these?
10. What proves extraction completeness and the boundary used for checkpoint advancement?

If these facts are unknown, the plan compiler must fail closed for semantics that depend on them.

## Capture semantics

| Capture pattern | What the source proves | Normal Bronze representation | Important limitation |
|---|---|---|---|
| Complete snapshot | State at a named snapshot boundary | Immutable `SNAPSHOT` | Changes between snapshots are unknown |
| Watermark | Rows matching a monotonic boundary predicate | `CURRENT_STAGE` or governed observation log | Physical deletes are normally invisible |
| Watermark with lookback | Re-observation of a recent boundary window | Deduplicated observation relation | Lookback reduces missed updates; it does not create delete visibility |
| Net-change CDC | Final supplied change per entity/window | `EVENT_LOG` with declared reduced fidelity | Intermediate changes may have been discarded upstream |
| All-change CDC | Ordered inserts, updates and deletes | Immutable ordered `EVENT_LOG` | Requires stable position, operation and tie handling |
| Business events/audit records | Distinct source events | Immutable `EVENT_LOG` | Requires stable event identity and replay rules |
| File drop | Whatever the file contract declares | Snapshot or event log plus immutable manifest | Arrival alone does not prove completeness or ordering |

CDC is a capture semantic, not a target load strategy. Captured CDC must feed a registered target
strategy such as APPEND, SCD1 or SCD2.

## Target semantics

| Target need | Spark strategy | Required source evidence |
|---|---|---|
| Immutable events or audit log | APPEND | Stable event identity and conflict policy |
| Complete replacement/current reference | REPLACE | Complete candidate and publication guards |
| Current state with insert/update/delete actions | UPSERT | Entity key and explicit delete semantics |
| Current state with overwrite-on-change semantics | SCD1 | Entity key and deterministic winner |
| Point-in-time entity history | SCD2 | Entity key, effective ordering and sufficient history fidelity |
| Changes inferred between complete snapshots | SNAPSHOT_DIFF | Two complete, comparable snapshots |
| Current view derived from authoritative history | CURRENT_PROJECTION | Ordered authoritative history and deterministic winner |

UPSERT and SCD1 may share an internal Spark kernel, but their public contracts should remain
distinct if their action and evidence semantics differ.

## Common enterprise scenarios

### 1. Nightly ERP full extract

An ERP exports all active products or cost centres once per night.

- Capture: complete snapshot with source boundary, manifest and completeness evidence.
- Bronze: immutable `SNAPSHOT` retained according to policy.
- Current target: REPLACE, or SNAPSHOT_DIFF followed by SCD1 when explicit change evidence is
  useful.
- Historical target: SNAPSHOT_DIFF followed by SCD2, with validity accurate to snapshot
  boundaries only.
- Delete handling: absence may mean deletion only when the snapshot contract proves completeness.

### 2. CRM modified-timestamp extraction

A CRM API returns accounts where `modified_at` is greater than a checkpoint.

- Capture: watermark with a frozen upper bound and usually a bounded lookback.
- Current target: SCD1 or UPSERT after deterministic deduplication.
- History: limited to observations returned by the API; it must not be described as complete
  change history.
- Delete handling: requires an explicit inactive/deleted flag, a separate tombstone feed or a
  periodic complete comparison. Hard deletes are otherwise invisible.

### 3. Operational database all-change CDC

A database log supplies ordered insert, update and delete events.

- Capture: immutable ordered `EVENT_LOG` keyed by source position and operation.
- Current target: UPSERT/SCD1 with a dataset delete policy.
- Historical target: SCD2 when ordering and effective-time rules are sufficient.
- Replay: the same source positions are no-ops; a reused identity with conflicting content fails
  closed.

### 4. Provider net-change feed

A SaaS provider supplies only the last state per entity within each polling window.

- Capture: `EVENT_LOG` only if it is clearly labelled net-change/reduced fidelity; otherwise use a
  current staging relation.
- Current target: SCD1/UPSERT.
- Historical target: may preserve delivered states, but cannot claim intermediate states that the
  provider discarded.
- Framework behavior: capability and evidence must expose this fidelity limitation.

### 5. Payments or orders as business events

A service emits authorizations, captures, refunds or order-status events.

- Capture: immutable `EVENT_LOG`.
- Identity: use source event ID or a documented composite; entity/order ID alone is insufficient.
- Target: APPEND for the ledger/event record, then CURRENT_PROJECTION for consumer-friendly state.
- Conflict: exact replay is a no-op; the same event identity with a different payload fails closed.

### 6. Application audit history

Systems such as ticketing or workflow products expose record history entries.

- Capture: APPEND using the source history record ID or an explicit composite event identity.
- Entity key: used for grouping and projection, not automatically for event deduplication.
- Projection: derive current state in Spark from the authoritative event relation.
- Warning: a whole-row hash is safe only if the source contract says identical observations are
  the same event.

### 7. Governed file delivery

A partner or internal system lands CSV, Parquet or Delta files.

- Capture: record immutable file identity, checksum, arrival time and run manifest.
- Snapshot file: require a completeness marker/manifest before REPLACE or absence-based deletes.
- Event file: require stable record identity and ordering before APPEND/SCD2.
- Replay: file replay and row replay must be handled independently.

### 8. Master/reference data publication to Silver

A small but business-critical hierarchy, exchange-rate set or policy mapping is rebuilt as a
complete candidate.

- Capture/transform: Spark still owns validation even when row count is small.
- Target: REPLACE through the selected Silver publication model.
- Guards: uniqueness, referential integrity, expected volume and freshness before cutover.
- Rollback: retain enough publication identity to restore the previously approved version.

### 9. Current projection from history

Consumers need one current row from an authoritative SCD2 or event history relation.

- Compute affected keys from Delta CDF when supported and certified.
- Read authoritative history at an explicit Delta version or stable boundary.
- Select the winner with complete deterministic ordering, not timestamp alone.
- Persist a normal Delta current table/view; never collect keys or rows to Python.

## Cross-pattern invariants

### Identity

- Entity key identifies the business object.
- Event identity identifies one immutable delivered event.
- Cursor/checkpoint identifies source progress.
- These may share columns in a specific source, but the metadata contract must not assume they are
  equivalent.

### Ordering

Timestamp alone is not deterministic when two changes have the same timestamp. Ordered processing
must declare a stable tie-breaker such as source sequence, log position or event ID. If no stable
order exists, the framework must use a documented conflict policy rather than accidental Spark row
order.

### Watermarks and baseline handoff

- Freeze an upper boundary before extraction where the source permits it.
- Advance a checkpoint only after the target commit and required reconciliation are proven.
- Commit checkpoints through compare-and-swap or equivalent lease fencing.
- A full baseline followed by incrementals requires explicit no-gap evidence: a source position
  captured with the baseline, overlap/replay coverage, or another source-supported boundary.
- Never set a checkpoint to “now” and assume the handoff is complete.

### Deletes

Watermark polling cannot infer a hard delete. Valid evidence includes a soft-delete field, CDC
delete operation, tombstone feed, complete-snapshot difference or separately governed audit. The
target action is an explicit dataset decision under ADR 0011; it is never inferred from the load
strategy.

### Late and backdated data

Arrival time and business effective time are different. SCD2 interval correction policy must be
explicit and must preserve the one-current-row and non-overlapping-interval invariants. ADR 0010
permits reject-and-rebuild or correction within a governed per-dataset window; unrestricted
retroactive correction is unsupported.

### Driver boundary

All joins, deduplication, hashes, diffs, interval logic, reconciliation and target mutation remain
inside Spark/Delta. Python receives bounded counts, status, commit identity and references to
persisted diagnostics. It never receives a complete dataset for these patterns.

## Pattern selection workflow

For each new dataset:

1. complete the source fact sheet;
2. select a truthful capture semantic and Bronze representation;
3. select target semantics based on consumer needs, not source technology names;
4. declare identity, ordering, delete, late-data, schema and replay policies;
5. compile the combination only if the runtime registry has a certified executor;
6. prove the pattern with local Spark/Delta tests and the required real Fabric UAT scenario; and
7. record fidelity limitations in dataset metadata and run evidence.

Unsupported fact combinations must fail plan compilation. They must not fall back to a Python
implementation or silently degrade to weaker semantics.
