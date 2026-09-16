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
last_reviewed: 2026-09-16
---

# Fabric delivery

## Domain-owned runtime boundary

Each domain owns one repository and isolated environment resources:

```text
domain repository
  -> domain Dev workspace  -> domain Dev control SQL Database
  -> domain UAT workspace  -> domain UAT control SQL Database
  -> domain Prod workspace -> domain Prod control SQL Database
```

The domain repository owns Fabric Pipelines, Copy activities, Dataflow Gen2 items, thin Notebooks
or Spark Job Definitions, Environments, Variable Libraries, schedules, metadata SQL and Gold code.
The framework repository owns reusable package code, SQL contracts and framework certification
assets only.

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

When native ingress is insufficient, one Spark capture job composes a registered source reader and
Bronze writer. A second generic Spark job consumes the completed capture for Silver by default, so
Silver retry does not contact the source again.

## Supported Pipeline shapes

### Native ingress and Silver in one Pipeline

```text
Copy or Dataflow Gen2 -> completed Bronze capture -> thin Spark launcher -> Silver
```

This is the normal enterprise batch topology and gives one observable Pipeline run.

### Spark capture and Silver in one Pipeline

```text
Spark capture job -> completed manifest -> generic Silver job
```

Use it for Delta Sharing, API, CDC or other protocols that native ingress cannot represent
truthfully.

### Decoupled producer and consumer Pipelines

Use only for fan-out, different schedules, independent service levels or operational scaling. The
producer persists an immutable manifest; consumers claim an explicit capture ID and MUST NOT select
“latest Bronze”.

## Pipeline grouping

Execution groups follow source, schedule, connection, provider concurrency and ownership, not load
strategy. One source Pipeline may contain mixed SCD1, SCD2, APPEND and REPLACE datasets. A source
with provider limits MAY be split into several Pipelines, but they share a concurrency key and
staggered schedules.

`FULL`, `WATERMARK` and `CDC` describe capture; they do not imply Silver behavior.

## Invocation handoff

Planning records domain, environment, dataset, physical bindings and calling item/run identities in
the local control plane. The per-dataset launcher passes only:

| Field | Purpose |
|---|---|
| `request_schema_version` | Launcher/request protocol schema version |
| `dataset_run_id` | Opaque frozen Silver-consumer run identity |
| `capture_id` | Explicit completed Bronze capture |
| `execution_request_id` | Optional approved rebuild/recovery request |

The runner resolves all strategy, policy, relation and binding values from immutable control state
and the manifest. The dataset's `contract_version` is already frozen inside `dataset_run_id`; it is
not repeated as a launcher parameter. Physical row payloads are never Pipeline parameters.

Spark Job Definition SHOULD be the stable production launcher. A Notebook MAY be used as a thin
parameter adapter and diagnostic surface, but MUST NOT contain load or reconciliation algorithms.

## Failure and replay

- Failed or partial captures are ineligible for Silver.
- Copy/Dataflow completion exposes a capture but does not advance the Silver checkpoint.
- Silver failure SHOULD retry the same capture rather than recapture.
- Replay follows the selected strategy's idempotency contract.
- CURRENT_STAGE MUST retain the manifest's pinned Delta version through its approved retry period.
- Concurrent consumers use claims and lease fencing.
- Checkpoint advancement follows target commit and reconciliation only.

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

1. publish the approved framework Environment/wheel;
2. apply compatible control-plane migration;
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

Real Fabric tests MUST cover Copy/Dataflow handoff, incomplete capture blocking, launcher argument
normalization, same-capture retry, connection/Lakehouse rebinding, exact wheel identity, control SQL
compatibility, OneLake/Delta behavior and end-to-end evidence linking Pipeline, capture, Spark and
target commit.
