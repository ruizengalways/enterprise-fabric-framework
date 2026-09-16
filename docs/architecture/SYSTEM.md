---
id: architecture.system
status: current
source_of_truth_for:
  - product-boundary
  - runtime-invariants
  - package-ownership
  - capability-support
last_reviewed: 2026-09-16
---

# System architecture

## Product boundary

Enterprise Fabric Framework is a Spark-first Microsoft Fabric framework for:

```text
enterprise source -> Bronze -> Silver
```

The framework ends after a governed Silver Delta commit, reconciliation and checkpoint/audit
completion. Domain repositories own Gold models, dimensional models, aggregates, KPIs, Power BI
semantic models, reports and consumer cutover.

The framework repository publishes a reusable wheel and control-plane SQL contract. Each domain
owns a separate repository, Dev/UAT/Prod workspaces, control SQL Databases, Fabric items, metadata
desired state and Gold implementation.

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
8. Checkpoints MUST advance only after target commit and required reconciliation are proven.
9. Certification MUST invoke production orchestration and runtime entry points.
10. Domain repositories MUST NOT inject private source-to-Silver Python plugins.
11. Fabric workspace item lifecycle and Gold behavior MUST remain outside the installed package.

## Production flow

```text
domain Fabric Pipeline
  -> plan execution group in the domain control SQL Database
  -> freeze effective metadata and bindings
  -> Fabric Copy/Dataflow Gen2
       or registered Spark source reader -> registered Bronze writer
  -> completed, validated Bronze capture manifest
  -> thin Spark Job Definition or Notebook launcher
  -> DatasetRunner
  -> SparkDatasetRuntime.run()
       -> transform -> load -> reconciliation
  -> bounded evidence
  -> audit and checkpoint commit
```

Fabric-native ingress is the normal path when it can land truthful source data. Spark capture is
optional and uses a source reader independently from the Bronze representation writer. Both paths
produce the same bounded capture-handoff contract.

## Public runtime contracts

The orchestration-to-Spark request contains identifiers and references, not rows:

- request-schema, Pipeline, capture and frozen run identities;
- effective configuration and binding hashes;
- source, Bronze, staging and Silver `RelationRef` values;
- a frozen source boundary or checkpoint window;
- selected registered capability versions; and
- schema, quality, reconciliation and load-policy references.

The runtime returns:

- terminal status and stable error code;
- bounded read, accepted, quarantined, filtered and mutation counts;
- Delta commit version or operation identity;
- reconciliation summary and detailed-evidence references; and
- proposed checkpoint and whether it is safe to commit.

It never returns source, target or quarantined business rows to Python.

## Capability registry

One Spark registry is the capability source of truth for source readers, Bronze writers,
transforms, load strategies, reconciliation and projections. Each identity is stable, versioned and
business-neutral. Planning fails closed when the installed wheel cannot resolve a selected
capability or when its declared request/evidence version and certification state are incompatible.

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
| `control_plane` | SQL access, frozen runs, leases, checkpoints and audit | Business data or physical binding logic |
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

sql/control_plane/     versioned reusable SQL schema and procedures
fabric/               framework certification items only
tests/unit/           bounded Python tests
tests/sql/            control-plane SQL contract tests
tests/spark/          local production-path Spark/Delta tests
tests/fabric/         real Fabric integration and release-gated UAT
```

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
- **Bronze representation**: what the retained capture truthfully means: SNAPSHOT, EVENT_LOG,
  CURRENT_STAGE or an approved EPHEMERAL exception.
- **Load strategy**: how a completed Bronze capture mutates Silver.
- **Dataset contract**: versioned source-to-Silver semantics and schema for one logical dataset.
- **Dataset run**: immutable compiled Silver-consumer execution plan.
- **Capture manifest**: bounded evidence that a specific Bronze boundary completed and is readable.
- **RelationRef**: logical relation identity resolved to an environment-specific physical relation.
- **Pattern capability**: registered reusable technical behavior implemented in the framework.
