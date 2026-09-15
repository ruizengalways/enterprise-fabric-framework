# Spark-first architecture

Status: proposed baseline for owner review.

## Purpose

The framework runs large business-data operations in Spark and Delta. Python coordinates the
work, but is not an alternative business-data execution engine.

## Product scope

The production boundary is `enterprise source -> Bronze -> Silver`. The runtime captures truthful
source data into Bronze and produces governed Silver Delta datasets. Silver is the final output of
this package.

Gold dimensional models, domain aggregates, KPIs, Power BI semantic models, T-SQL serving views
and domain release mechanisms live in separately owned domain repositories. The common framework
must not acquire Gold abstractions merely because multiple domain repositories consume Silver.

Silver outputs are registered physical Delta tables by default. Spark views are permitted for
internal or explicitly Spark-only intermediate relations, not as the default cross-domain
contract. ADR 0009 defines the full ownership boundary.

Compatible refreshes keep the same stable Silver contract. Breaking corrections or semantic/schema
changes use parallel versioned Silver contracts so existing consumers remain undisturbed while
domain repositories validate and adopt the replacement. The migration model is described in
[`SILVER_VERSIONING.md`](SILVER_VERSIONING.md); ordinary REPLACE does not create a new contract
version for every refresh.

## Production flow

```text
logical DatasetConfig + EnvironmentBindings
                    |
                    v
             ExecutionPlan
                    |
                    v
          SparkDatasetRunRequest
                    |
                    v
         SparkDatasetRuntime.run()
                    |
       +------------+-------------+
       |            |             |
   capture       transform       load
       |            |             |
       +------------+-------------+
                    |
          Spark reconciliation
                    |
                    v
   bounded SparkRunEvidence + status
                    |
                    v
   control-plane audit/checkpoint commit
```

## Non-negotiable rules

1. Spark/Delta is the only production implementation of business-data semantics.
2. No production `apply/` package or row-list fallback exists.
3. Data crosses runtime boundaries as a DataFrame or named relation, never `list[dict]`.
4. Driver collection is limited to bounded scalar aggregates, `LIMIT 1` guards and explicitly
   capped diagnostic samples.
5. `toPandas()` is forbidden in production source.
6. Target mutation, reconciliation and checkpoint evidence are tied to one dataset run ID.
7. Checkpoints advance only after target commit is proven and required reconciliation passes.
8. A capability is advertised only when a concrete registered runtime and required tests exist.
9. Dev, UAT and Prod use the same wheel and runtime entry point.
10. Certification must call the same public Spark runtime used by production.
11. Pure-Python in-memory business algorithms are forbidden in source, tests, certification and
    fallback paths. The framework does not maintain a second semantic implementation.
12. CDC delete behavior, Bronze representation, event identity and schema evolution are explicit
    dataset contracts; missing or incompatible semantics fail before target execution.

## Runtime contracts

The orchestration-to-Spark request contains identifiers and references, not rows:

- pipeline and dataset run IDs;
- effective configuration and plan hashes;
- source/staging/target relation references;
- a frozen source boundary or checkpoint window;
- schema, quality, reconciliation and apply policy references;
- expected bounded counts when independently available.

The Spark runtime returns bounded evidence:

- terminal status and stable error code;
- rows read, accepted, quarantined and filtered;
- insert/update/delete/replay counts;
- Delta target commit version or operation identity;
- reconciliation summary and references to persisted detailed evidence;
- proposed checkpoint and whether it is safe to commit.

The runtime does not return target rows, incoming rows, quarantined rows or partition-wide
metric collections to Python.

## Accepted dataset semantics

- SCD2 late arrivals use an explicit per-dataset `REJECT_AND_REBUILD` or
  `CORRECT_WITHIN_WINDOW` policy. Bounded correction requires authoritative effective ordering;
  unrestricted retroactive correction is unsupported.
- CDC-capable datasets explicitly select `HARD_DELETE`, `SOFT_DELETE`, `SCD2_CLOSE`, `IGNORE` or
  `REJECT`; executors never infer delete meaning.
- Fabric Copy may land source-faithful Bronze data and a bounded capture manifest. Spark owns all
  normalization, quality, reconciliation and Silver mutation.
- Every dataset declares `SNAPSHOT`, `EVENT_LOG`, `CURRENT_STAGE` or governed `EPHEMERAL` Bronze
  representation.
- APPEND and event capture declare stable event identity. Canonical row content is identity only
  when the source contract explicitly gives identical observations that meaning.
- Schema evolution is `STRICT` by default. Explicit `ADDITIVE_NULLABLE` policy may admit compatible
  additions; breaking changes require a reviewed contract version.

The binding decision for these policies is recorded in ADRs 0010 through 0015.

## Load strategies

Source capture facts, enterprise scenarios and the evidence required before choosing a target
strategy are defined in [`DATA_PATTERNS.md`](DATA_PATTERNS.md). Strategy names must not be selected
without those source semantics.

- APPEND: identity validation, conflict detection and insert-only Delta MERGE.
- REPLACE: a run-scoped physical Delta candidate, Spark validation and governed publication to the
  stable Silver relation under ADR 0020.
- UPSERT and SCD1: two public semantics backed by one current-state Spark kernel.
- SCD2: ordered affected-key processing, close-current/insert-new actions and temporal invariants.
- SNAPSHOT_DIFF: distributed hash/full-outer-join diff with complete-snapshot evidence.
- CURRENT_PROJECTION: Delta CDF affected keys plus authoritative history time travel.
- CDC: a capture mode producing an ordered Bronze Delta relation for a registered load strategy.

## Environment promotion

Dataset semantics are environment-independent. An environment resolver binds logical names to
workspace, Lakehouse, catalog/schema, connection and secret references. Environment selection
must not branch inside strategy implementations.

## Operational commands

Git stores durable dataset semantics and environment bindings; it never stores a pending or
consumed `REBUILD` command. Rebuild and future recovery operations are bounded, one-time
`ExecutionRequest` records in the control plane under ADR 0019.

The team-facing production path is:

```text
GitHub manual workflow
  -> plan and freeze source/bindings/artifact
  -> protected-environment approval
  -> create/approve one-time ExecutionRequest
  -> Fabric Pipeline(execution_request_id)
  -> Spark Job Definition(--execution-request-id)
  -> public DatasetRunner/runtime
  -> terminal evidence
```

The Fabric boundary receives only the opaque request ID. Compare-and-swap state transitions and
dataset lease fencing prevent duplicate execution. A Git push may test and deploy code/configuration
but cannot automatically create a production rebuild request.

## Testing model

The complete test design, execution tiers and target directory layout are defined in
[`TESTING_STRATEGY.md`](TESTING_STRATEGY.md) and ADR 0007.

- Python unit tests cover contracts, configuration, orchestration and control-plane state.
- Local Spark/Delta tests cover every business-data strategy through the public runtime entry.
- Nightly tests cover replay, concurrency, failure injection and larger data volumes.
- Real Fabric UAT covers OneLake, catalog, identity, Job Definition, Pipeline and runtime drift.

Expected results are asserted through fixed fixtures, Spark relational comparison, Delta history
and semantic invariants. A second in-memory business algorithm is not a test oracle.

Local Spark/Delta tests prove the portable Spark implementation, not the complete Fabric platform.
A local PASS cannot by itself certify Fabric because it does not reproduce OneLake storage,
Fabric catalog resolution, managed identity, Spark Job Definition and Pipeline invocation, the
managed runtime build, or provider-specific concurrency and retry behavior. Real Fabric UAT must
therefore execute the exact candidate wheel through the same public runtime entry before a
strategy is certified for production.

The test suite must not contain Python-versus-Spark parity tests. Those tests would require a
second business engine, increase consistency maintenance cost and still leave Fabric-specific
behavior unproven.

The initial compatibility target is Fabric Runtime 2.0 with Spark 4.1, Delta 4.2, Python 3.13 and
Java 21. Local CI pins the closest available matching open-source patch versions. Spark-only or
experimental Delta 4.2 table features are not enabled for cross-domain Silver contracts without
separate interoperability certification.

## CI/CD and Fabric deployment

GitHub Actions is the single top-level CI/CD orchestrator because GitHub is the source repository.
It owns pull-request validation, local Spark/Delta tests, exact-wheel construction, artifact
identity, environment approvals and the end-to-end deployment record.

The delivery path is:

```text
pull request
  -> Python unit tests + local Spark/Delta tests
  -> build exact versioned wheel and SHA256
  -> deploy Fabric item definitions with fabric-cicd
  -> upload and publish the exact wheel through stable Fabric Environment APIs
  -> resolve and verify target Environment/Lakehouse/Job bindings
  -> run Fabric Dev smoke tests
  -> deploy to UAT and run real Fabric Spark UAT
  -> approve the protected production environment
  -> deploy the same wheel bytes to Prod and run a production smoke test
```

Fabric Deployment Pipelines may be used when required by organizational governance. In that mode,
GitHub Actions invokes and observes the native pipeline through Fabric APIs. The native pipeline
remains a deployment mechanism; it does not become a second CI/CD authority and does not rebuild
the candidate artifact.

GitHub Environments own stage-specific approvals and credentials. Repository environment manifests
remain the deployment input. A Fabric Variable Library may materialize stage-specific values, but
must not become a separately maintained duplicate of the same configuration.

Every deployment records the Git SHA, wheel version and SHA256, configuration and binding hashes,
Fabric item identities, Fabric runtime version, provider deployment identity and Spark execution
identity. Production release is prohibited unless real Fabric UAT refers to that exact candidate.

## Definition of production support

A strategy is production-supported only when all of the following are true:

1. it is registered in the Spark runtime registry;
2. the registry can resolve a concrete executor;
3. local Spark/Delta tests execute the public runtime path;
4. required reconciliation is Spark-native and bounded at the driver;
5. failure and replay behavior is tested;
6. the strategy has passed the required real Fabric UAT scenario.
