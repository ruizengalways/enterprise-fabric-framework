---
id: architecture.testing
status: current
source_of_truth_for:
  - test-boundaries
  - local-spark-testing
  - fabric-uat
  - ci-tiers
last_reviewed: 2026-09-16
---

# Testing strategy

## Objectives

Tests MUST execute the production Spark/Delta implementation directly, give fast PR feedback and
prove Fabric-specific behavior before release. They MUST NOT maintain a Python, pandas, SQL or
alternative-engine implementation of business semantics as an oracle.

```text
one production package
  -> local Spark/Delta in developer and GitHub CI
  -> exact candidate wheel in real Microsoft Fabric
```

## Python unit tests

Python-only tests cover bounded concerns:

- immutable contracts and small-object hashing;
- frozen SQL row-to-policy mapping and validation;
- logical binding resolution;
- registry/plan consistency and dependency scheduling;
- leases, checkpoints and compare-and-swap state;
- operational request approval/claim/expiry/retry;
- bounded evidence validation;
- Fabric invocation normalization; and
- capture-manifest schema and completion gates.

They MUST NOT recreate APPEND, SCD, CDC, diff or projection results with row lists, pandas or a
second local engine.

## Control-plane SQL tests

SQL tests apply versioned migrations to an isolated disposable database and exercise public
procedures, idempotent metadata desired state, policy validation, frozen runs, leases, checkpoints
and concurrency slots.

They verify that metadata deployment does not copy or overwrite environment-local control state.
They MUST NOT load business tables or reimplement Spark strategies in T-SQL. Real Fabric SQL
Database UAT remains required for managed security, identity and compatibility.

## Local Spark and Delta tests

Every pull request starts local Spark/Delta, creates temporary relations and invokes the same public
runtime used by Fabric. `local[2]` is sufficient for semantic tests; multi-node/provider behavior
belongs in Fabric.

Coverage includes:

- FULL, WATERMARK, CDC and locally representable Delta CDF boundaries;
- independent source-reader/Bronze-writer resolution and incompatible combinations;
- EVENT_LOG identity replay and conflict;
- immutable SNAPSHOT boundaries;
- CURRENT_STAGE full replacement/keyed merge and pinned-version replay;
- normalization, schema, quality and quarantine;
- APPEND, REPLACE, UPSERT, SCD1, SCD2 and SNAPSHOT_DIFF;
- current projection and affected-key processing;
- reconciliation and checkpoint gates;
- idempotent replay and failure-before-mutation;
- rebuild through the public runtime; and
- Delta history and operation evidence.

Tests use business-neutral pattern names and fixtures. A payment-originated requirement is tested as
version ordering, soft-delete decoding or another reusable pattern, not as payment business logic.

## Runtime image

Developers and GitHub Actions SHOULD use the same pinned Docker image aligned to the baseline in
`FABRIC_DELIVERY.md`. Exact image digest and open-source patch versions are locked when the image is
built. Docker standardizes execution but does not certify OneLake, Fabric catalogs, identity or
managed-runtime behavior.

## Test data

Small literal fixtures MAY be converted immediately into Spark DataFrames. They are inputs, not an
alternative algorithm. Large fixtures MUST be generated with Spark expressions such as
`spark.range`; tests MUST NOT build large `list[dict]` values or convert results to pandas.

## Result validation

Fixed expected relations are compared in both directions:

```sql
SELECT * FROM actual
EXCEPT ALL
SELECT * FROM expected
```

```sql
SELECT * FROM expected
EXCEPT ALL
SELECT * FROM actual
```

Both differences must be empty. Additional invariants include:

- unique event identity and conflicting replay rejection;
- unique current-state merge key;
- exactly one current SCD2 row per active key;
- non-overlapping validity intervals and open current end boundary;
- replay leaves the committed target unchanged;
- failure does not advance the checkpoint;
- Delta commit and mutation evidence agree; and
- read/accepted/quarantined/filtered accounting closes.

Property/metamorphic tests MAY verify replay, stable ordering and batch splitting, but MUST express
invariants over Spark results rather than compute a second Python result.

## Real Fabric UAT

Fabric UAT is a release gate for the exact candidate wheel:

```text
SETUP    isolated source, target and control state
EXECUTE  native or Spark capture -> production DatasetRunner
VERIFY   Spark relational comparisons and invariants
RETAIN   bounded result plus detailed Delta evidence references
```

Notebooks remain thin investigation launchers. Framework certification runs in framework-owned
test workspaces; adopting domains repeat required scenarios in their isolated UAT workspace and
control database.

Fabric UAT proves risks absent locally: OneLake, catalog/workspace resolution, managed identity,
Environment publication, Job/Pipeline invocation, native retries, Fabric SQL Database behavior,
runtime drift and provider concurrency.

## Isolation and cleanup

Every integration run uses a unique `test_run_id`. Local tests use temporary paths; Fabric tests
use isolated schemas or prefixes. Successful temporary state may be removed after evidence is
recorded. Failed state is retained for a diagnostic TTL and removed by a separate idempotent cleanup
workflow.

## CI tiers

| Tier | Trigger | Required work |
|---|---|---|
| PR fast | Every pull request | unit, SQL contract and core local Spark/Delta |
| Main full | Merge to main | full SQL/Spark plus exact wheel and migration artifact |
| Nightly | Schedule | replay, random batches, failure injection, concurrency and larger volume |
| Fabric UAT | Candidate/main/nightly | exact wheel in real Fabric |
| Production smoke | Deployment | bounded read-only or idempotent verification |

## Target directories

```text
tests/
  unit/
    contracts/
    metadata/
    orchestration/
    control_plane/
    platform/
  sql/
    migrations/
    metadata_contract/
    planning/
    concurrency/
  spark/
    fixtures/
    capture/sources/
    capture/writers/
    transform/
    load/
    reconciliation/
    projection/
  fabric/
    ingress/
    environment/
    job_definition/
    lakehouse/
    bindings/
    invocation/
    uat/
```

Directories are created with their first executable test.

## Required CI guards

PR CI MUST fail for an unregistered advertised capability, prohibited in-memory engine,
`toPandas()`, unbounded collection, failing SQL/Spark contract, business-named capability,
incompatible semantic change or unbuildable package.

Fabric UAT MUST fail for wheel hash mismatch, unresolved binding, wrong control schema/metadata
hash, copied control state, invalid capture manifest, implicit latest-Bronze lookup, unreadable
CURRENT_STAGE pinned version, invalid operational request, failed reconciliation or incomplete
deployment evidence.
