---
id: product.usage-model
status: current
source_of_truth_for:
  - intended-users
  - domain-workflows
  - pipeline-usage
  - dataset-onboarding
  - development-and-promotion
last_reviewed: 2026-09-18
---

# Expected usage model

## Purpose

This document describes how teams are expected to use the framework in real work. Architecture and
capabilities are developed to serve these workflows, not as abstract infrastructure.

Every material framework feature SHOULD support a use case in this document. A pull request adding
a source, writer, transform, load strategy or operational feature SHOULD reference the relevant use
case and prove it through the test tier defined in `architecture/TESTING.md`.

## Intended users

| User | Main responsibility | How they use the framework |
|---|---|---|
| Domain data engineer | Onboard and operate Source-to-Silver datasets | Fabric UI, domain metadata SQL, thin launchers and framework capabilities |
| Framework engineer | Implement reusable technical patterns | Framework Python/SQL code, local Spark tests and Fabric certification |
| Platform/release engineer | Maintain environments and promotion | GitHub Actions, Fabric lifecycle tooling, bindings and approvals |
| Operator/support engineer | Diagnose, pause, retry or rebuild | Fabric monitoring, control-plane evidence and governed operations |
| Gold/reporting engineer | Consume stable Silver contracts | Domain-owned Gold repository and independent consumer deployment |

## Enterprise example

A customer domain needs to load 40 tables from four source systems:

1. an enterprise database supported by Fabric Copy;
2. a SaaS source implemented with Dataflow Gen2;
3. Delta Sharing with 20 tables and a provider-side connection/concurrency limit; and
4. an HTTP API requiring Spark-native capture.

The expected domain design is five Pipelines, not one giant Pipeline and not one Pipeline per load
strategy:

```text
customer-copy                  database tables through Fabric Copy
customer-dataflow              SaaS tables through Dataflow Gen2
customer-delta-share-01        first Delta Sharing group
customer-delta-share-02        second Delta Sharing group
customer-api                   Spark-native API capture
```

Each Pipeline owns its Fabric schedule and operational monitoring. The two Delta Sharing groups
share a `source_concurrency_key` and staggered schedules so splitting the workload does not bypass
the provider limit.

The domain repository owns these five Pipelines, metadata SQL and Gold code. The framework
repository owns the reusable Delta Sharing/API readers and Bronze/Silver processing patterns.

## Expected domain repository

```text
customer-data-platform/
  fabric/
    pipelines/
    notebooks/                 thin diagnostic launchers only
    spark_jobs/                thin production launchers
    environments/
    variable_libraries/
  sql/
    metadata/                  ordered idempotent desired-state SQL
    validation/
  gold/                        business/domain logic begins here
  tests/
    fabric/
    gold/
  .github/workflows/
```

The domain repository MUST NOT contain private Source-to-Silver reader, SCD or load implementations.
If a reusable technical behavior is missing, it is developed in the framework and released before
the domain promotes metadata that selects it.

## UC-01: Fabric-native ingress followed by Silver processing

Use this when Copy or Dataflow Gen2 can append source-faithful Bronze deliveries.

```text
Fabric schedule
  -> domain Pipeline
  -> plan execution group through the domain control-plane adapter
  -> Copy activity or Dataflow Gen2
  -> publish append-only Bronze with required delivery evidence
  -> invoke generic Silver Spark Job
       request_schema_version
       silver_run_id
  -> DatasetRunner
  -> Structured Streaming using the contract's stable checkpoint
  -> selected Spark micro-batch load/reconciliation
  -> bounded evidence
```

The domain engineer creates and maintains the Copy/Dataflow activity in the Fabric UI. The package
does not create it through an API. The activity lands source-faithful Bronze only; normalization,
quality, SCD and Silver mutation happen in the framework Spark runtime.

The Pipeline MAY process several tables in one scheduled execution group. Each producer and Silver
consumer receives its own frozen job plan and evidence. A Silver query's checkpoint persists across
scheduled job runs; another Silver version uses a different checkpoint.

## UC-02: Spark-native source followed by Silver processing

Use this for Delta Sharing, specialized APIs, unsupported connectors or source protocols needing
Spark control.

```text
Fabric schedule
  -> domain Pipeline
  -> plan execution group
  -> generic Spark capture job
       registered source reader
       + registered Bronze writer
  -> append-published Bronze
  -> generic Structured Streaming Silver Spark Job
  -> bounded evidence and Spark-managed checkpoint
```

Capture and Silver are separate jobs by default. If Silver fails, the team resumes its query from
the same checkpoint without reconnecting to the source.

A Delta Sharing reader, for example, does not select Silver semantics. Metadata selects a
compatible append-only Bronze writer independently; source facts determine whether retained rows
represent complete snapshots, observations or ordered events.

For an API without streaming support, the engineer selects a finite extraction window and the
reader fetches its pages to completion, then appends the delivery to Bronze. Silver consumes that
relation through its normal streaming query. See
[Bounded Source-to-Bronze extraction](architecture/DATA_LIFECYCLE.md#bounded-source-to-bronze-extraction)
for the source contract; this does not introduce another Silver execution mode.

## UC-03: Multiple tables with different Silver strategies

A Pipeline is grouped by source and operations, not SCD strategy. One execution group may contain:

| Table | Capture | Bronze | Silver |
|---|---|---|---|
| `customer_event` | CDC/events | `EVENT_LOG` | APPEND |
| `customer` | watermark | `EVENT_LOG` observations | SCD1 |
| `customer_address` | ordered changes | `EVENT_LOG` | SCD2 |
| `country_reference` | full snapshot | `SNAPSHOT` | REPLACE |

The Pipeline does not call `load.scd1()` or `load.scd2()` directly. It passes opaque job-run identities
to the generic launcher. The frozen SQL metadata selects the registered micro-batch load executor.

Conceptually, domain metadata contains:

```text
source_to_bronze_config: crm_address:1
capture_mode: CDC
bronze_representation: EVENT_LOG

bronze_to_silver_config: customer_address:1
source_to_bronze_config_ref: crm_address:1
load_strategy: SCD2
checkpoint_ref: customer_address_v1
identity_policy: customer_id
ordering_policy: source_version + event_id
delete_policy: SCD2_CLOSE
```

The actual domain source of truth is idempotent SQL calling public metadata procedures, not YAML.
Bronze policies/rules and Silver policies/rules reference their respective configurations internally;
[Control-plane policy fields](architecture/CONTROL_PLANE.md#policy-fields-and-rules) defines their storage and pattern settings.
Domain SQL supplies logical dataset IDs and versions; public procedures resolve internal IDs, so
engineers do not manage environment-specific surrogate keys. The metadata model is canonical in
`architecture/CONTROL_PLANE.md`.

## UC-04: Fast Dev development and debugging

Developers do not need to run CI/CD for every metadata edit.

Expected loop:

1. edit the domain's idempotent metadata SQL file;
2. execute that same file manually against the domain Dev control-plane instance;
3. compile/inspect the frozen plan for one dataset or execution group;
4. run the Dev Pipeline or thin diagnostic Notebook/Spark Job;
5. inspect Bronze, Silver, Delta history and bounded evidence in Dev;
6. repeat until behavior is correct; and
7. commit the final SQL and Fabric item changes for review.

The Notebook is a parameter and investigation surface. Business algorithms MUST remain in the
installed framework wheel, so Dev debug and production use the same implementation.

## UC-05: Promote an existing pattern to UAT and Prod

When all required capabilities already exist:

```text
Dev metadata/Fabric item change
  -> Dev run
  -> pull request
  -> SQL idempotency + plan validation + domain tests
  -> promote Fabric items and metadata to UAT
  -> domain Fabric UAT using pinned framework wheel
  -> Prod approval
  -> promote the same definitions and activate
```

The domain does not need a framework release for every new table. Dev, UAT and Prod run the same
domain definitions and wheel; environment bindings resolve different physical resources.

## UC-06: Introduce a new reusable technical pattern

Suppose five new tables contain a source version column and soft-delete marker, but current
capabilities cannot interpret them correctly.

Expected workflow:

1. describe the behavior generically, without table/business names;
2. implement or compose a framework capability such as `latest_by_version@1` and
   `soft_delete_marker@1`;
3. add neutral local Spark/Delta tests and required Fabric certification;
4. build and publish one immutable framework wheel;
5. pin that wheel in the domain Dev/UAT boundary;
6. add domain metadata selecting the new capability; and
7. promote the framework before dependent domain metadata reaches UAT/Prod.

Do not create `CustomerPaymentTransform` or keep the new function in the domain repo. If the same
behavior applies to credit-card payments, accounts or products without knowing their business
meaning, it belongs in the framework under a pattern name.

## UC-07: Temporarily stop or permanently retire a table

The engineer pauses the selected producer or consumer using the configuration's `is_enabled` field
through public metadata procedures. See [Enablement and retirement](architecture/CONTROL_PLANE.md#enablement-and-retirement)
for switch semantics, already running jobs and preserving pause settings across deployments.

For example, stopping Silver v1 can leave the Source-to-Bronze producer and Silver v2 enabled.
Temporary pauses retain checkpoints for resume; retirement requires consumer confirmation and
governed cleanup. Removing a SQL statement from Git does not silently delete or disable a dataset.

## UC-08: Build Silver v2 without disturbing v1

If Bronze remains valid:

```text
Bronze -> Silver v1 -> existing Gold/report consumers
       -> Silver v2 -> validation -> migrated consumers
```

If Bronze is also wrong:

```text
source -> Bronze v1 -> Silver v1 -> existing consumers
      -> Bronze v2 -> Silver v2 -> validation -> migrated consumers
```

Create a new Bronze-to-Silver configuration for the v2 data contract with its own checkpoint when
valid Bronze can be reused. When Bronze also needs correction, copy and correct the source-table
configuration, register a new Source-to-Bronze configuration and Bronze relation, and attach Silver
v2 to it.

v1 and v2 run independently; matching progress is unnecessary. If comparison is needed, the
engineer chooses a cutoff date explicitly. Domain Gold/reporting code chooses when to move to v2.
The framework does not repoint Gold objects. After permanent v1 decommissioning its checkpoint can
be removed under [Source-to-Bronze configuration](architecture/CONTROL_PLANE.md#source-to-bronze-configuration); a temporary pause
retains it. Shared Bronze remains available for other consumers and replay.

## UC-09: Rebuild an existing Silver contract

A rebuild is an operational action, not a permanent metadata flag:

```text
GitHub manual workflow
  -> dry-run plan with frozen Bronze/source boundary
  -> protected approval
  -> one-time execution_request_id
  -> production Fabric Pipeline and DatasetRunner
  -> reconciliation and terminal evidence
```

The detailed operator procedure is in `operations/REBUILD.md`. Changing grain or semantics is a v2
migration, not a rebuild.

## UC-10: Diagnose a failed or ambiguous run

The operator starts from the Pipeline run and bounded framework evidence:

1. locate Pipeline, Bronze/Silver run and Spark query/batch identities;
2. determine published Bronze input and available delivery/completeness evidence;
3. inspect Delta commit/history and persisted reconciliation references;
4. confirm checkpoint state and lease/request ownership;
5. resume the same query checkpoint using the replay-safe load path; and
6. preserve failed state until diagnostic evidence is retained.

The operator MUST NOT resolve ambiguity by deleting/editing a checkpoint or running an unrelated
full replay into the existing target.

## Development priority rule

Framework work is prioritized in this order:

1. behavior required by a concrete Source-to-Silver use case;
2. reusable contracts and patterns shared by several datasets/domains;
3. operational safety, evidence and testability needed to run those cases in Fabric; and
4. optional abstractions only after a demonstrated need.

A proposed feature with no actor, workflow, dataset pattern or production test scenario SHOULD NOT
be implemented merely to make the framework appear more general.
