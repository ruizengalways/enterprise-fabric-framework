---
id: architecture.fabric-delivery
status: current
source_of_truth_for:
  - domain-workspaces
  - fabric-ingress
  - invocation-handoff
  - environment-promotion
  - package-cicd
  - fabric-runtime-baseline
last_reviewed: 2026-09-18
---

# Fabric delivery

## Domain-owned runtime boundary

Each domain owns one repository and isolated environment resources:

```text
domain repository
  -> domain Dev workspace  -> domain Dev control-plane instance
  -> domain UAT workspace  -> domain UAT control-plane instance
  -> domain Prod workspace -> domain Prod control-plane instance
```

Each Workspace is the deployment/domain boundary for its control plane, Delta relations, Spark
checkpoints and runtime state. The package does not receive a `domain_id`; the Workspace-bound
adapter and connection establish scope.

The domain repository owns Fabric Pipelines, Copy activities, Dataflow Gen2 items, thin Notebooks
or Spark Job Definitions, Environments, Variable Libraries, schedules, metadata SQL and Gold code.
The framework repository owns reusable package code, SQL contracts and framework certification
assets only. Its native Fabric test items belong to
[the Fabric test resource layout](TESTING.md#target-directories).

Dev is the authoring environment. UAT and Prod MUST be promoted from reviewed definitions rather
than independently rebuilt by hand. Credentials, gateways and identities remain environment-owned
and outside Git.

## Fabric item lifecycle

The installed Python package MUST NOT create, update, delete, deploy or discover Fabric workspace
items. Normal lifecycle is:

1. author items in the domain's Git-connected Dev workspace;
2. commit native item definitions to the domain repository;
3. review them with metadata SQL;
4. promote with Fabric Deployment Pipelines or approved Fabric-native automation;
5. verify environment-specific connections and destinations; and
6. execute smoke/UAT checks after promotion.

External CI MAY use Fabric REST APIs, Fabric CLI or `fabric-cicd`; those clients remain deployment
automation, not installed framework capabilities.

## Source-to-Bronze ownership

Fabric Copy and Dataflow Gen2 SHOULD be used for source-faithful Bronze ingress when their
connectors satisfy the source contract. They MUST NOT implement curated business transformations or
write Silver APPEND, REPLACE, UPSERT, SCD1 or SCD2 targets.

Spark exclusively owns normalization, deterministic ordering, row hashing, quality decisions,
reconciliation, CDC interpretation, snapshot diff, projection and Silver mutation.

When native ingress is insufficient, one Spark producer job composes a registered source reader
and append-only Bronze writer. A generic Silver Spark job independently consumes the retained
Bronze relation through Structured Streaming, so Silver retry does not contact the source again.

Copy/Dataflow or an API without a streaming reader may complete one finite extraction per producer
invocation. The meaning and requirements of this pattern are canonical in
[Bounded Source-to-Bronze extraction](DATA_LIFECYCLE.md#bounded-source-to-bronze-extraction).
The following Pipeline shapes use the same Silver streaming runtime for either producer type.

## Supported Pipeline shapes

### Native ingress and Silver in one Pipeline

```text
Copy or Dataflow Gen2 -> append-published Bronze -> thin streaming Spark launcher -> Silver
```

This is the normal enterprise batch topology and gives one observable Pipeline run.

### Spark capture and Silver in one Pipeline

```text
Spark producer job -> append-published Bronze -> generic streaming Silver job
```

Use it for Delta Sharing, API, CDC or other protocols that native ingress cannot represent
truthfully.

### Decoupled producer and consumer Pipelines

Use for different schedules, independent service levels or operational scaling. Each Silver
consumer reads the configured Bronze relation with its own stable Spark checkpoint. Consumers do
not require paired runs or explicit capture-ID claims. See `DATA_LIFECYCLE.md` for execution and
snapshot publication semantics.

## Pipeline grouping

Execution groups follow source, schedule, connection, provider concurrency and ownership, not load
strategy. One source Pipeline may contain mixed SCD1, SCD2, APPEND and REPLACE datasets. A source
with provider limits MAY be split into several Pipelines, but they share a concurrency key and
staggered schedules. The metadata contract and group-boundary rules are defined in the
[control-plane execution group design](CONTROL_PLANE.md#execution-group-design).

`FULL`, `WATERMARK` and `CDC` describe capture; they do not imply Silver behavior.

## Invocation handoff

Planning records environment, dataset, physical bindings and calling item/run identities in the
local control plane. The per-dataset launcher passes only:

| Field | Purpose |
|---|---|
| `request_schema_version` | Launcher/request protocol schema version |
| `silver_run_id` | Opaque frozen Bronze-to-Silver run identity |
| `execution_request_id` | Optional approved rebuild/recovery request |

Source-to-Bronze launchers use an opaque `bronze_run_id` instead of `silver_run_id`. The runner resolves
strategy, policy, Bronze relation and checkpoint binding from immutable control state. There is no
mandatory `capture_id` argument. The dataset's `contract_version` is already frozen inside
`silver_run_id`; it is not repeated as a launcher parameter. Physical row payloads are never
Pipeline parameters.

Scheduled Silver jobs SHOULD use the execution model in
`DATA_LIFECYCLE.md#structured-streaming-execution`; normal stream startup resumes from its checkpoint
and consumes available input. A frozen replay cutoff is a separate one-time request, not an offset
reset on every Pipeline invocation.

Spark Job Definition SHOULD be the stable production launcher. A Notebook MAY be used as a thin
parameter adapter and diagnostic surface, but MUST NOT contain load or reconciliation algorithms.

## Failure and replay

- Failed or partial snapshot deliveries are ineligible for snapshot-dependent Silver publication.
- Copy/Dataflow appends Bronze but does not advance a Silver query's checkpoint.
- Silver failure SHOULD resume the same query checkpoint without recapturing the source.
- Replay follows the selected strategy's idempotency contract.
- Checkpoint ownership, concurrency and cleanup follow `CONTROL_PLANE.md`.

## Environment-independent code

Dev, UAT and Prod run the same package bytes. Logical `RelationRef` values are resolved with
environment bindings and Variable Libraries. Business behavior MUST NOT branch on environment
name. Intentional differences such as concurrency or alert destination require typed operational
settings.

## Framework package CI/CD

GitHub Actions is the authority for the Python package:

```text
pull request
  -> unit + SQL + local Spark/Delta
  -> build one exact wheel and SHA256
  -> install exact wheel in approved Fabric Environment
  -> Fabric Dev smoke
  -> real Fabric UAT
  -> protected Prod approval
  -> promote the same wheel bytes
  -> production smoke
```

GitHub Environments separate credentials and approvals. Package CI and Fabric item promotion are
complementary lifecycles; Fabric Deployment Pipelines do not replace package build/test, and the
package does not replace Fabric item lifecycle.

Domain repositories pin the framework version and hash. When a table uses existing capabilities,
only the domain metadata/items need promotion. A new capability requires the framework wheel and
compatible SQL migration to be published before dependent domain metadata reaches UAT or Prod.

The environment deployment order is:

1. publish the approved framework Environment/wheel and control-plane adapter;
2. apply compatible default-SQL migration, or deploy the equivalent adapter contract;
3. deploy desired metadata and Fabric items;
4. validate bindings and plan compilation;
5. execute smoke/UAT; and
6. explicitly enable a new risky dataset.

## Fabric Runtime baseline

Verified against Microsoft Learn on 2026-09-16:

| Component | Baseline |
|---|---|
| Microsoft Fabric Runtime | 2.0 GA |
| Apache Spark | 4.1 |
| Delta Lake | 4.2 |
| Python | 3.13 |
| Java | 21 |
| Scala | 2.13 |

Dev, UAT and Prod Environments MUST explicitly select Runtime 2.0 while it is not the Fabric
default. Local CI uses the closest pinned open-source versions but is not a Fabric emulator.

Delta 4.2 features that are experimental or Spark-only MUST remain disabled for cross-experience
Silver tables unless separately certified for every declared consumer.

Runtime upgrades are reviewed changes: build a candidate baseline, run local suites, certify the
exact wheel in Fabric, then update this canonical document. Builds MUST NOT float on `latest`.

Primary references:

- [Microsoft Fabric Runtime 2.0](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-2-0)
- [Apache Spark runtimes in Fabric](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime)

## Fabric UAT obligations

Real Fabric tests MUST cover append-only Copy/Dataflow handoff, incomplete snapshot blocking,
launcher argument normalization, checkpoint resume, independent v1/v2 queries, connection/Lakehouse
rebinding, exact wheel identity, the selected control-plane adapter, OneLake/Delta behavior and
end-to-end evidence linking Pipeline, Bronze/Silver runs, Spark and target commit. The default SQL
adapter additionally requires Fabric SQL Database compatibility checks.
