# Testing strategy

Status: proposed baseline for owner review.

## Objectives

The test system must:

- execute the production Spark/Delta implementation directly;
- provide fast pull-request feedback;
- keep local development and GitHub Actions reproducible;
- prove Fabric-specific behavior before release;
- avoid a second Python, pandas or alternative-engine implementation of business semantics;
- retain enough evidence to diagnose failed or ambiguous target commits.

## Test environments

```text
                         one production package
                                  |
                 +----------------+----------------+
                 |                                 |
        local Spark/Delta                    Microsoft Fabric
                 |                                 |
        developer + Docker              exact candidate wheel
                 |                                 |
          GitHub Actions CI              Spark Job Definition UAT
```

### Python unit tests

Python-only tests cover bounded logic that does not process business tables:

- configuration parsing and validation;
- immutable contracts and hashing;
- capability and runtime-registry consistency;
- execution-plan compilation and dependency scheduling;
- control-plane leases, checkpoints and compare-and-swap behavior;
- one-time execution-request planning, approval, claim, expiry and retry behavior;
- audit and bounded evidence validation;
- Fabric REST request, response and error handling.

They must not recreate load, CDC or projection results with lists, dictionaries, pandas or another
local engine.

### Local Spark and Delta

GitHub Actions runs Spark in local mode for every pull request. These tests create temporary Delta
relations, invoke the public production runtime and validate results with Spark and Delta.

The initial implementation should use `local[2]`. A multi-node Docker Compose cluster is not
required for semantic tests. Distributed concurrency, OneLake storage and provider retries belong
in Fabric UAT.

Local tests cover:

- FULL, WATERMARK, CDC and Delta CDF capture boundaries that can be represented locally;
- Bronze normalization, schema enforcement, data quality and quarantine routing;
- APPEND, REPLACE, UPSERT, SCD1, SCD2 and SNAPSHOT_DIFF;
- REPLACE candidate isolation, validation-before-publication and stable-target/checkpoint behavior;
- current projection and affected-key behavior;
- reconciliation invariants and checkpoint gates;
- idempotent replay and failure-before-mutation behavior;
- rebuild execution through the same public runtime with a frozen Bronze boundary;
- Delta history and operation evidence.

### Docker

Docker standardizes the test runtime. The initial image targets Java 21, Python 3.13, Spark 4.1 and
Delta 4.2 to match Fabric Runtime 2.0 as closely as the open-source distributions permit. Exact
patch versions and image digests are pinned after dependency resolution.

The same image is used by developers and GitHub Actions. Docker does not define expected business
results and is not treated as Fabric certification. Runtime-specific or Spark-only Delta table
features require separate Fabric interoperability tests before Silver uses them.

### Real Fabric UAT

Real Fabric UAT is a release gate. GitHub Actions installs the exact candidate wheel into a Fabric
Environment, publishes it, resolves target bindings and invokes a Spark Job Definition that calls
the same `SparkDatasetRuntime.run()` entry used by local tests and production.

The preferred UAT phases are:

```text
SETUP
  create isolated source, target and control state for test_run_id

EXECUTE
  invoke the production DatasetRunner Spark Job Definition

VERIFY
  run Spark relational comparisons and invariants
  persist bounded result and detailed Delta evidence references

RETAIN
  upload evidence to the GitHub run and preserve failed Fabric state for investigation
```

Notebooks may call a repository-owned UAT runner for manual investigation. They must remain thin
and must not contain separate test implementations of APPEND or SCD semantics.

## Test data generation

Small fixtures may be declared as literal records and converted immediately into a Spark
DataFrame. This is input data, not a business algorithm.

Large deterministic fixtures are generated in Spark:

```python
source = (
    spark.range(row_count)
    .selectExpr(
        "cast(id as string) as business_key",
        "concat('value-', id) as value",
        "timestamp'2026-09-15 10:00:00' as modified_at",
        "id as source_sequence",
    )
)
```

Tests must not generate large `list[dict]` inputs or convert Spark results to pandas.

## Result validation without a second engine

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

Both results must be empty. Strategy-specific assertions also include:

- unique APPEND identity and conflict rejection;
- unique current-state merge key;
- exactly one current SCD2 row per active business key;
- non-overlapping SCD2 validity intervals;
- current SCD2 rows have an open end boundary;
- replay leaves the target relation unchanged;
- failed reconciliation does not advance the checkpoint;
- Delta commit version and mutation evidence agree;
- source, accepted, quarantined and filtered accounting closes.

Property and metamorphic tests may verify replay, stable ordering and valid batch splitting. They
must express invariants over the Spark result rather than calculate a second expected result with
Python business logic.

## Isolation and retained evidence

Every Spark integration run uses a unique `test_run_id`. Local tests use temporary filesystem
locations. Fabric tests use isolated schemas or table prefixes such as:

```text
uat_<test_run_id>.source
uat_<test_run_id>.target
uat_<test_run_id>.quarantine
uat_<test_run_id>.reconciliation_details
```

Successful ephemeral state may be removed after evidence is recorded. Failed state is retained for
a configured diagnostic period and deleted by a separate idempotent TTL cleanup workflow.

## CI tiers

| Tier | Trigger | Required work | Purpose |
|---|---|---|---|
| PR fast | Every pull request | unit + core local Spark/Delta | fast semantic and contract feedback |
| Main full | Merge to main | full local Spark/Delta + build exact wheel | candidate construction |
| Nightly | Scheduled | replay, randomized batches, failure injection, concurrency, larger volumes | deeper regression evidence |
| Fabric UAT | Main/nightly/release candidate | exact wheel in real Fabric | platform compatibility and release gate |
| Production smoke | Production deployment | bounded read-only or idempotent checks | deployment verification |

## Target test and automation directories

```text
docker/
└── spark-test/
    └── Dockerfile

.github/
└── workflows/
    ├── ci.yml
    ├── nightly-spark.yml
    ├── fabric-uat.yml
    └── production-deploy.yml

tests/
├── unit/
│   ├── config/
│   ├── contracts/
│   ├── metadata/
│   ├── orchestration/
│   ├── control_plane/
│   └── platform/
├── spark/
│   ├── conftest.py
│   ├── fixtures/
│   ├── capture/
│   ├── transform/
│   ├── load/
│   │   ├── test_append.py
│   │   ├── test_replace.py
│   │   ├── test_upsert.py
│   │   ├── test_scd1.py
│   │   ├── test_scd2.py
│   │   └── test_snapshot_diff.py
│   ├── reconciliation/
│   └── projection/
├── fabric/
│   ├── environment/
│   ├── job_definition/
│   ├── lakehouse/
│   └── bindings/
└── uat/
    ├── scenarios/
    ├── expected/
    └── verifier/
```

Directories are created when their first executable test or workflow is implemented. Empty
directory scaffolding is not required.

## Required workflow behavior

The PR workflow must fail when:

- production modules import a prohibited in-memory business engine;
- a capability has no concrete runtime registration;
- production source calls `toPandas()` or performs an unbounded driver collection;
- local Spark/Delta business tests fail;
- the package cannot be built and installed into a clean environment.

The Fabric UAT workflow must fail when:

- the installed wheel identity differs from the candidate SHA256;
- the Environment, Lakehouse or Spark Job binding is unresolved;
- an operational request is unapproved, stale, expired or already claimed;
- provider success lacks a durable framework outcome;
- reconciliation fails or the checkpoint advances unsafely;
- retained evidence does not bind the Git SHA, wheel, runtime, item identities and Spark run.

## Fabric-specific limitations

A local Spark PASS does not prove OneLake behavior, Fabric catalog and workspace binding, managed
identity, Environment publication, Spark Job Definition invocation, native retries or provider
concurrency. These are exclusively certified by the real Fabric UAT tier.

## References

- [ADR 0001: Spark and Delta are the only production business-data runtime](../adr/0001-spark-delta-only-production-runtime.md)
- [ADR 0006: GitHub Actions controls CI/CD](../adr/0006-github-actions-controls-cicd.md)
- [ADR 0007: Test the production Spark path](../adr/0007-test-production-spark-path.md)
