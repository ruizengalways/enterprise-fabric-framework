---
id: architecture.control-plane
status: current
source_of_truth_for:
  - sql-metadata
  - typed-policies
  - frozen-runs
  - checkpoints-and-leases
  - operational-requests
last_reviewed: 2026-09-16
---

# Control plane

## Domain isolation

Every domain has a separate control SQL Database in Dev, UAT and Prod. There is no enterprise-wide
mutable runtime database. Business rows MUST remain in Fabric ingress storage or Spark/Delta
relations; the SQL Database holds bounded metadata and operational state only.

```text
metadata.*   reviewed desired state
control.*    environment-local runtime state
```

CI/CD may migrate both schemas, but MUST promote only `metadata.*` desired state. Runs,
checkpoints, leases, requests, captures and suspensions MUST NOT be copied between environments.

## SQL-first metadata delivery

Domain metadata is ordered, idempotent SQL in the domain repository. It calls versioned public
framework procedures rather than writing private tables directly. Developers MAY execute the same
file manually in Dev for rapid feedback. UAT and Prod accept it only through reviewed CI/CD.

The framework does not compile domain YAML, publish a configuration bundle to OneLake or scan Git
files during a Spark run. Runtime reads deployed SQL state and freezes it before execution.

Removing a row from Git MUST NOT silently delete a deployed dataset. Disablement, retirement and
contract replacement are explicit desired-state operations.

## Metadata model

The control database records its owning Domain once:

```text
metadata.domain_context
  domain_id                       one row for the database ownership boundary
```

Dataset versions are routing contracts, so the table is named `dataset_contract` rather than
`dataset`. One logical dataset can have v1 and v2 rows during a parallel migration:

```text
metadata.dataset_contract
  dataset_contract_id             immutable primary key
  dataset_id
  contract_version                positive major version: 1, 2, ...
  execution_group_id
  source_profile_id
  bronze_relation_ref
  silver_relation_ref
  capture_mode
  bronze_representation
  load_strategy
  is_enabled

UNIQUE(dataset_id, contract_version)
```

`domain_id` is intentionally absent from each contract row because every Domain owns a different
control database. `metadata.domain_context` lets procedures validate that the caller and database
match without repeating the value in every primary/foreign key.

`metadata.source_profile` and `metadata.execution_group` are shared routing definitions referenced
by contracts. Concern-specific policy tables reference `dataset_contract_id`:

```text
metadata.source_profile
metadata.execution_group

metadata.identity_policy
metadata.ordering_policy
metadata.watermark_policy
metadata.delete_policy
metadata.snapshot_policy
metadata.event_log_policy
metadata.current_stage_policy
metadata.append_policy
metadata.scd2_policy
metadata.replace_policy
metadata.snapshot_diff_policy
metadata.schema_policy
metadata.quality_policy
metadata.reconciliation_policy
```

`dataset_contract_id` is an internal immutable database identity. Domain engineers and desired-state
SQL address a contract by `(dataset_id, contract_version)` through public procedures; those
procedures resolve or create the internal ID and attach its policies. Scripts do not hard-code a
different surrogate ID for Dev, UAT and Prod.

For example:

```text
metadata.identity_policy
  dataset_contract_id
  entity_key_columns
  event_identity_columns

metadata.scd2_policy
  dataset_contract_id
  effective_time_column
  tie_breaker_columns
  late_arrival_policy
```

Required fields use real columns. Bounded lists such as key, tie-breaker and tracked columns MAY use
validated JSON arrays. A catch-all JSON document MUST NOT be the primary model; rare connector
options require a versioned schema.

Watermark, source version, SCD2 effective time, tie breaker and delete marker are separate semantic
roles even when a dataset maps them to the same source column.

## Environment binding

Dataset metadata uses typed logical `RelationRef` values. Environment-owned binding rows and
Fabric Variable Libraries resolve them to workspace, Lakehouse, catalog/schema/table, connection
and secret identities. Strategy code MUST NOT branch on an environment name or construct topology.

Missing, ambiguous or cross-environment bindings fail before Spark target mutation. Secrets remain
outside Git and SQL desired-state scripts. A physical setting MUST NOT be maintained independently
in both SQL metadata and a Variable Library.

## Execution-group planning

Pipelines group datasets by source, schedule, connection, provider concurrency and operational
ownership, not by Silver strategy. A group may contain APPEND, REPLACE, SCD1 and SCD2 tables.

The Pipeline calls a bounded planner similar to:

```text
control.usp_plan_execution_group(
  execution_group_id,
  environment,
  fabric_pipeline_run_id
)
```

The planner validates enabled metadata, registry support and bindings, then creates one immutable
`control.dataset_run` per Silver consumer execution. The Pipeline receives bounded routing values;
it does not interpret SCD policies or private metadata joins.

The frozen run contains resolved capability versions, policy values, relation references, hashes
and boundaries. Mid-run metadata edits cannot alter it.

## Contract versions

`contract_version` is the major version of the dataset's consumer-facing data contract. It is not
the framework wheel version, a registered capability version or a source record's version column.

```text
customer_address:1
  Bronze  = bronze.crm_address_events
  Silver  = silver.customer_address_v1
  key     = customer_id + address_id

customer_address:2
  Bronze  = bronze.crm_address_events
  Silver  = silver.customer_address_v2
  key     = customer_id + address_type + address_sequence
```

When valid Bronze can serve both contracts, v1 and v2 reference the same Bronze relation/capture
and produce separate Silver relations. If Bronze semantics are also wrong, v2 references a new
Bronze contract.

The runtime does not look for a `v2.py` or YAML file. Planning selects enabled
`metadata.dataset_contract` rows, joins their typed policies by `dataset_contract_id`, and freezes
one `control.dataset_run` per selected contract. The generic runtime then resolves the same
registered Spark executor with different frozen keys, schema and target relation.

During migration, both versions may be enabled. After consumer cutover, v1 is disabled and later
retired through governed cleanup. A routine refresh, rebuild, schedule change, performance refactor
or compatible nullable-column addition does not create a new contract version. Breaking grain,
keys, history/delete semantics, canonical type meaning or consumer behavior does.

Operators SHOULD be able to inspect versions through public views such as:

```text
metadata.v_dataset_contract
control.v_dataset_run
```

Run evidence includes `dataset_id`, `contract_version`, `dataset_contract_id`, resolved Bronze and
Silver relations, framework wheel identity and selected capability versions.

## Plan compilation

Compilation MUST fail before capture or target mutation when:

- a selected capability/version is absent or uncertified;
- Bronze representation or writer is missing/incompatible;
- EVENT_LOG or APPEND lacks event identity;
- SNAPSHOT lacks a complete named boundary;
- CURRENT_STAGE lacks compatible key, ordering, delete or retention policy;
- SCD2 lacks entity key, deterministic ordering or one compatible SCD2 policy;
- REPLACE lacks complete-candidate and publication policy;
- a watermark policy appears under an incompatible capture mode; or
- irrelevant strategy policy rows would otherwise be ignored.

## Operational state

Planned bounded state includes:

```text
control.pipeline_run
control.dataset_run
control.capture_manifest
control.checkpoint
control.lease
control.execution_request
control.dataset_suspension
control.audit_event
```

The exact capture/run identity relationship remains an open decision because a single authoritative
capture may need to feed Silver v1 and v2. See `../decisions/OPEN.md`.

## Capture manifests

A completed manifest contains bounded metadata only:

- contract, dataset/capture and provider run identities;
- completion state;
- landed relation, Delta version, partition or immutable file references;
- frozen source boundary and Bronze representation;
- source schema/contract identity;
- available bounded source/copied/rejected counts; and
- completion time and evidence references.

The consumer validates the manifest before reading Bronze. A provider activity marked successful
is insufficient when the manifest is missing, partial or inconsistent.

## Checkpoints, leases and concurrency

Checkpoint advancement requires a proven target commit and passed reconciliation. Updates use
compare-and-swap or equivalent lease fencing. A successful capture alone does not advance the
Silver checkpoint.

Dataset/capture claims prevent concurrent duplicate consumers. Pipelines sharing a provider use a
common `source_concurrency_key`, bounded control-plane slots and deliberate scheduling. Splitting a
provider across Pipelines MUST NOT bypass its connection limit.

## Bounded evidence

Production code MAY collect only bounded scalar aggregates, `LIMIT 1` existence guards and
explicitly capped diagnostic samples. Detailed reconciliation, conflict and quarantine data is
written as Delta and returned by reference.

Evidence binds at least:

- Git commit and wheel version/SHA256;
- metadata, plan and environment-binding hashes;
- Pipeline, capture, dataset-run and Spark execution identities;
- Fabric runtime version;
- source boundary and resolved relations;
- target Delta commit;
- reconciliation outcome; and
- checkpoint decision.

## One-time operational requests

REBUILD and recovery operations are not durable dataset configuration. They use immutable,
auditable `control.execution_request` records with plan, approval, claim, expiry, retry and terminal
states.

Production normally uses:

```text
GitHub manual workflow
  -> plan and freeze boundary/artifact/bindings
  -> protected-environment approval
  -> create approved execution request
  -> Fabric Pipeline(execution_request_id)
  -> public DatasetRunner/runtime
  -> terminal evidence
```

The Fabric launcher receives only the opaque request ID. A Git push MUST NOT automatically create a
production rebuild. Reusing a successful request is a no-op; changing source boundary, code,
configuration or binding requires a new request.

A request freezes environment, dataset/contract, operation, source boundary, artifact identity,
configuration/binding hashes, requester, approver, reason/ticket and expiry. State transitions use
compare-and-swap and follow a bounded model such as:

```text
PLANNED -> APPROVED -> CLAIMED -> RUNNING -> SUCCEEDED | FAILED | CANCELLED
    \-> REJECTED                                 \-> EXPIRED
```

One approved request can be claimed once. Prod execution requires protected-environment approval;
request creation alone is never authorization.

## SQL implementation ownership

Reusable schema, procedure and migration artifacts belong under `sql/control_plane/`. Production
domain desired-state scripts belong under `sql/metadata/` in the domain repository. The framework
SQL implementation MUST include isolated-database tests and real Fabric SQL Database UAT, and MUST
never implement Spark business-table semantics in T-SQL.
