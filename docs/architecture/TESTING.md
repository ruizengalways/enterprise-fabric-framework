---
id: architecture.testing
status: current
source_of_truth_for:
  - test-boundaries
  - local-spark-testing
  - fabric-uat
  - ci-tiers
last_reviewed: 2026-09-18
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
- `ControlPlanePort` contract behavior and replaceable adapter wiring;
- query leases, reader-mode/checkpoint-reference validation, physical checkpoint-path uniqueness
  and compare-and-swap state;
- operational request approval/claim/expiry/retry;
- bounded evidence validation;
- Fabric invocation normalization; and
- Bronze-manifest schema and completion gates.
- Silver-manifest batch identity, retry deduplication, commit correlation and summary availability.

They MUST NOT recreate APPEND, SCD, CDC, diff or projection results with row lists, pandas or a
second local engine.

## Control-plane SQL tests

SQL tests apply versioned migrations to an isolated disposable database and exercise public
procedures, idempotent metadata desired state, policy validation, frozen runs, query leases
and concurrency slots. Policy storage and pattern requirements follow
[Control-plane policy fields](CONTROL_PLANE.md#policy-fields-and-rules). Checks include one policy per configuration,
rule ownership, watermark lookback versus snapshot completeness, and rejection of inapplicable fields.

They verify reader-declared bounded/streaming checkpoint nullability and rejection of shared physical
checkpoint paths. They also verify that metadata deployment does not copy or overwrite
environment-local control state.
They MUST NOT load business tables or reimplement Spark strategies in T-SQL. Real Fabric SQL
Database UAT remains required for managed security, identity and compatibility.

## Local Spark and Delta tests

Every pull request starts local Spark/Delta, creates temporary relations and invokes the same public
runtime used by Fabric. `local[2]` is sufficient for semantic tests; multi-node/provider behavior
belongs in Fabric.

Coverage includes:

- FULL, WATERMARK, CDC and locally representable Delta CDF boundaries;
- independent source-reader/Bronze-writer resolution and incompatible combinations;
- bounded producer window/pagination completion followed by the normal Silver streaming runtime;
- source cursor advancement after Bronze publication, independent of Silver query progress;
- append-only Bronze record-identity replay and conflict;
- immutable SNAPSHOT completion, empty snapshots and boundaries spanning micro-batches;
- Structured Streaming AvailableNow, checkpoint resume and source offset evidence;
- streaming producer checkpoint resume with independent producer/Silver checkpoint paths;
- independent v1/v2 consumption and corrected Bronze chains;
- normalization, schema, quality and quarantine;
- APPEND, REPLACE, UPSERT, SCD1, SCD2 and SNAPSHOT_DIFF;
- current projection and affected-key processing;
- reconciliation before successful micro-batch completion;
- failure after target commit and before checkpoint completion with replay-safe recovery;
- Silver manifest recovery after SQL recording failure, retaining original commit metrics;
- reconciliation failure followed by replay, retaining failed/successful attempts and one logical result;
- checkpoint ownership and permanent decommissioning cleanup;
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
- a failed callback does not mark its micro-batch complete;
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
    items/                     native Pipeline, Notebook, Spark Job and Environment definitions
    ingress/
    environment/
    job_definition/
    lakehouse/
    bindings/
    invocation/
    uat/
```

Framework-owned native Fabric definitions MUST live under `tests/fabric/items/`; they are
integration/UAT test resources deployed into isolated framework test workspaces. The other
`tests/fabric/` directories contain test drivers and assertions that exercise those resources
through the public production runtime. Thin launchers MUST call the installed framework wheel
rather than contain a separate business-data implementation.

The framework repository has no root-level `fabric/` resource directory. Domain production Fabric
items remain owned by domain repositories as described in `../USAGE_MODEL.md#expected-domain-repository`.
Reusable SQL schema, procedures and migrations remain under `sql/control_plane/`.

Directories are created with their first executable test or native test item.

## Required CI guards

PR CI MUST fail for an unregistered advertised capability, prohibited in-memory engine,
`toPandas()`, unbounded collection, failing SQL/Spark contract, business-named capability,
incompatible semantic change or unbuildable package.

Fabric UAT MUST fail for wheel hash mismatch, unresolved binding, wrong control schema/metadata
hash, copied control state, invalid snapshot publication evidence, shared query checkpoint paths,
unreadable replay offsets, invalid operational request, failed reconciliation or incomplete
deployment evidence.
