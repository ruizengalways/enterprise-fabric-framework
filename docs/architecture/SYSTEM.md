---
id: architecture.system
status: current
source_of_truth_for:
  - product-boundary
  - runtime-invariants
  - package-ownership
  - capability-support
last_reviewed: 2026-09-18
---

# System architecture

## Product boundary

Enterprise Fabric Framework is a Spark-first Microsoft Fabric framework for:

```text
enterprise source -> Bronze -> Silver
```

The framework ends after a governed Silver Delta commit, reconciliation and query/audit completion
evidence. Domain repositories own Gold models, dimensional models, aggregates, KPIs, Power BI semantic
models, reports and consumer cutover.

The framework repository publishes a reusable wheel, a logical control-plane contract and a default
SQL adapter. Each domain owns a separate repository, Dev/UAT/Prod workspaces, a control-plane
implementation, Fabric items, metadata desired state and Gold implementation.

## Non-negotiable invariants

1. Spark and Delta MUST be the only production implementation of business-data semantics.
2. Production code and tests MUST NOT implement a second row-list, pandas or pure-Python business
   engine for APPEND, REPLACE, UPSERT, SCD1, SCD2, SNAPSHOT_DIFF, CDC or projection.
3. Business data MUST cross package boundaries as Spark DataFrames, Spark SQL or declared Delta
   relations, never as `list[dict]` or `Sequence[Mapping]`.
4. Production code MUST NOT call `toPandas()` or collect a business table, affected-key set,
   quarantine set or unbounded metric set to the driver.
5. Driver results MUST be bounded scalar evidence, status and references to persisted details.
6. Dev, UAT and Prod MUST run the same wheel and public runtime path; only bindings and approved
   environment settings differ.
7. A capability MUST NOT be advertised unless a concrete registry executor and required test
   evidence exist.
8. Spark owns streaming checkpoints; required micro-batch writes and reconciliation MUST complete
   before the callback returns successfully.
9. Certification MUST invoke production orchestration and runtime entry points.
10. Domain repositories MUST NOT inject private source-to-Silver Python plugins.
11. Fabric workspace item lifecycle and Gold behavior MUST remain outside the installed package.

## Production flow

```text
domain Fabric Pipeline
  -> plan execution group through the domain control-plane adapter
  -> freeze effective metadata and bindings
  -> Fabric Copy/Dataflow Gen2
       or registered Spark source reader -> registered append-only Bronze writer
  -> published, retained Bronze Delta relation
  -> thin Spark Job Definition or Notebook launcher
  -> DatasetRunner
  -> SparkDatasetRuntime.run()
       -> Structured Streaming micro-batches
       -> transform -> load -> reconciliation
  -> bounded evidence
  -> batch evidence and Spark-managed checkpoint recovery
```

Fabric-native ingress is the normal path when it can land truthful source data. Spark capture is
optional and uses a source reader independently from the Bronze representation writer. Both paths
publish source-faithful append-only Bronze. Independent Silver versions consume that relation
without synchronized capture handoff; execution semantics are canonical in `DATA_LIFECYCLE.md`.

## Public runtime contracts

The orchestration-to-Spark request contains identifiers and references, not rows:

- request-schema, Pipeline and frozen Bronze/Silver run identities;
- effective configuration and binding hashes;
- source, Bronze, staging and Silver `RelationRef` values and logical checkpoint references;
- source selection and optional approved replay cutoff;
- selected registered capability versions; and
- schema, quality, reconciliation and load-policy references.

The runtime returns:

- terminal status and stable error code;
- bounded read, accepted, quarantined, filtered and mutation counts;
- Delta commit version or operation identity;
- reconciliation summary and detailed-evidence references; and
- query/batch completion and checkpoint-reference evidence.

It never returns source, target or quarantined business rows to Python.

The runtime depends on the typed control-plane port, not on SQL table names or a particular control
database. The package provides a default SQL-backed implementation; a company or domain-owned
adapter MAY persist equivalent plan, lease, run and evidence operations elsewhere.
Custom adapters MUST preserve the port's idempotency, fencing, checkpoint and bounded-evidence
semantics and pass the same contract tests.

## Capability registry

One Spark registry is the capability source of truth for source readers, Bronze writers,
transforms, load strategies, reconciliation and projections. Each identity is stable, versioned and
business-neutral. A source-reader registration MUST declare whether it executes as bounded
extraction or Structured Streaming, including whether the producer `checkpoint_ref` is required;
planning validates that declaration and the resolved binding. Planning fails closed when the
installed wheel cannot resolve a selected capability or when its declared request/evidence version
and certification state are incompatible.

Capabilities are named by reusable technical patterns such as `delta_sharing_snapshot@1`,
`latest_by_version@1` or `soft_delete_marker@1`, never after a customer, payment, finance or other
business table. New company-required source-to-Silver behavior is implemented and released in this
framework before dependent domain metadata reaches UAT or Prod.

A registered transform MUST accept/return Spark DataFrames or declared relation plans, declare
additional inputs/outputs, avoid undeclared external side effects, and contribute its identity and
version to the plan hash/evidence. It SHOULD be deterministic for the same declared inputs unless
its contract explicitly says otherwise. The initial product exposes no external callback/plugin SDK;
a new table first composes existing operators before a new framework capability is introduced.

## Package ownership

| Package | Owns | Must not own |
|---|---|---|
| `contracts` | Immutable cross-layer values | Spark actions or SQL connections |
| `metadata` | Typed mapping of frozen SQL policies and capability selections | SQL access, executors or domain seed data |
| `utils` | Small deterministic primitives with no I/O | Spark row algorithms or workflows |
| `spark/capture/sources` | Spark-native connector/protocol reads | Bronze mutation or Silver semantics |
| `spark/capture/writers` | Bronze representation persistence and capture evidence | Connector protocols or Silver mutation |
| `spark/transform` | Normalization, quality and distributed row expressions | Driver-side business-row processing |
| `spark/load` | Registered Silver Delta mutation | Pipeline orchestration or durable control state |
| `spark/reconciliation` | Distributed checks and detailed evidence relations | Unbounded driver evidence |
| `spark/projection` | Derived relations from authoritative Spark/Delta data | Gold business models |
| `control_plane` | Control-plane port, default SQL adapter, frozen runs, query leases, checkpoint references and audit | Business data, streaming offsets/state or physical binding logic |
| `orchestration` | Planning, claims, dependencies and runtime coordination | Strategy algorithms |
| `platform/fabric` | Invocation normalization and physical-binding resolution | Fabric item CRUD or deployment |
| `recovery` | Governed checkpoint/target recovery decisions | A second load implementation or Gold cutover |
| `certification` | Thin scenarios invoking public production paths | Alternative business algorithms |
| `cli` | Operator commands over public APIs | Direct control-table edits or another runtime path |

Driver-side UTC helpers and hashes of bounded contracts belong in `utils`. Distributed canonical
row hashing belongs in `spark/transform` and MUST use Spark expressions over a declared schema.

## Repository shape

```text
src/enterprise_fabric_framework/
  contracts/
  metadata/
  control_plane/
  orchestration/
  platform/fabric/
  spark/
    capture/sources/
    capture/writers/
    transform/
    load/
    reconciliation/
    projection/
  recovery/
  certification/
  cli/
  utils/

sql/control_plane/     default SQL adapter schema, views, procedures and migrations
tests/unit/           bounded Python tests
tests/sql/            control-plane SQL contract tests
tests/spark/          local production-path Spark/Delta tests
tests/fabric/         real Fabric integration and release-gated UAT
  items/              native Fabric definitions used by those tests
```

Fabric test resource ownership and layout are canonical in
[Testing](TESTING.md#target-directories).

Directories SHOULD be added with their first executable artifact rather than retained as empty
placeholders.

## Production support definition

A capability is production-supported only when it:

1. has a stable registry identity and concrete executor;
2. is reachable through the public runtime;
3. has production-path local Spark/Delta tests;
4. keeps reconciliation and evidence bounded at the driver;
5. proves replay and failure behavior; and
6. passes the required real Fabric UAT using the exact candidate wheel.

## Glossary

- **Capture mode**: how a source boundary is obtained, such as FULL, WATERMARK or CDC.
- **Bronze representation**: what append-retained source facts mean: SNAPSHOT or EVENT_LOG;
  EPHEMERAL is reserved and unavailable.
- **Load strategy**: how streamed Bronze facts mutate Silver.
- **Source-to-Bronze configuration**: versioned producer definition in `metadata.source_to_bronze_config`.
- **Bronze-to-Silver configuration**: versioned consumer definition in `metadata.bronze_to_silver_config`
  referencing a Source-to-Bronze configuration.
- **Dataset contract**: data semantics, schema and compatibility of one logical dataset version;
  this is a data-contract concept rather than a metadata table name.
- **Bronze run**: immutable compiled plan and execution record for one Source-to-Bronze invocation.
- **Silver run**: immutable compiled plan for one Silver query invocation; its checkpoint outlives
  individual invocations.
- **Bronze manifest**: bounded delivery/completeness evidence, required for named snapshots.
- **Silver manifest**: bounded result evidence for one logical micro-batch; defined in
  [Execution evidence](CONTROL_PLANE_EVIDENCE.md#silver-manifests).
- **Streaming checkpoint**: Spark-owned persisted query progress and state, unique per query contract.
- **RelationRef**: logical relation identity resolved to an environment-specific physical relation.
- **Pattern capability**: registered reusable technical behavior implemented in the framework.
