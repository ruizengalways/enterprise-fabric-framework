---
id: examples.control-plane-configuration
status: illustrative
last_reviewed: 2026-09-18
---

# Control-plane configuration examples

Read [Control plane](../architecture/CONTROL_PLANE.md) for the canonical table model, ownership,
validation and delivery rules. This document provides example values and annotated SQL, not another
runtime contract. Tables/procedures are planned; the SQL below is not runnable until implemented.
Numeric configuration IDs illustrate relationships and do not prescribe SQL PK storage types.

## Worked configuration example

Assume the customer Domain copies watermark observations from CRM into Bronze, then runs Silver
SCD1. The routing definitions and environment bindings below have already been deployed.

| Source-to-Bronze column | Example | Meaning / how to populate |
|---|---|---|
| `source_to_bronze_config_id` | `101` (illustrative generated ID) | Database-generated configuration PK; 101 only illustrates references and is not supplied in desired-state SQL. |
| `dataset_id` | `crm_customer` | Stable logical name for the source dataset; not a run ID or physical table name. |
| `contract_version` | `1` | Data-contract version of an independent ingestion chain; routine parameter edits do not automatically increment it. |
| `execution_group_id` | `crm_daily` | Execution group that plans this producer configuration. |
| `source_profile_description` | `CRM / Fabric connection crm / dbo.Customer` | Source documentation for inspection; neither native nor package execution uses it to route reads. |
| `source_reader_ref` | `NULL` | Copy uses a Fabric activity; package ingress requires a registered reader and version. |
| `source_connection_ref` | `NULL` | Copy uses its Fabric connection; supply a logical reference when a package reader requires one. |
| `bronze_relation_ref` | `bronze.crm_customer_v1` | Logical Bronze destination, resolved to a physical table independently in Dev/UAT/Prod. |
| `checkpoint_ref` | `NULL` | This Copy example uses bounded extraction; streaming producers require a separate checkpoint reference. |
| `capture_mode` | `WATERMARK` | Extract records within the invocation's declared watermark window. |
| `bronze_representation` | `EVENT_LOG` | Append-retain observations; complete snapshots use SNAPSHOT, with semantics defined in [Data lifecycle](../architecture/DATA_LIFECYCLE.md#bronze-representations). |
| `is_enabled` | `1` | Eligible for future normal planning; does not mean execution has already succeeded. |

`bronze_relation_ref` and `checkpoint_ref` identify different resources: the former locates business
data, the latter locates Spark-owned progress/state. A reader-declared streaming producer would use
an example `checkpoint_ref` of `checkpoints.crm_customer_bronze_v1`; it differs from its Silver
consumers' checkpoints in the planned model. Bounded producers keep this field `NULL` and use the
control-plane source cursor where applicable.

| Bronze-to-Silver configuration | Producer reference | Silver target ref | Checkpoint ref | Strategy |
|---|---|---|---|---|
| `customer:1` | `crm_customer:1` | `silver.customer_v1` | `checkpoints.customer_silver_v1` | `SCD1` |
| `customer:2` | `crm_customer:1` | `silver.customer_v2` | `checkpoints.customer_silver_v2` | `SCD1` |

The two consumer rows have different generated PKs and reference the same producer PK internally.
Here v2 corrects the entity-key policy. Each consumer has its own policies, execution-group
membership and `is_enabled`; v2 can be validated on an independent schedule. If Bronze extraction
also needs correction, create `crm_customer:2` with a separate Bronze relation and producer progress,
and register the consumer against that new producer version. Version numbers do not create physical
resources; domain Fabric definitions/bindings must provide the new destinations.

## Annotated desired-state SQL

The following procedure names/signatures are illustrative until implemented under
`sql/control_plane/`; this example is not yet runnable and does not replace required policy SQL.

```sql
-- Store in the domain repository's sql/metadata/; upsert idempotently by logical dataset/version.
EXEC metadata.usp_upsert_source_to_bronze_config
    @dataset_id = N'crm_customer',                 -- Stable logical source dataset name
    @contract_version = 1,                        -- Producer contract version, not a run ID
    @execution_group_id = N'crm_daily',            -- Previously registered execution group
    @source_profile_description = N'CRM / Fabric connection crm / dbo.Customer', -- Documentation only
    @source_reader_ref = NULL,                    -- Copy source settings belong to its Fabric activity
    @source_connection_ref = NULL,                -- Required only when the package reader needs one
    @bronze_relation_ref = N'bronze.crm_customer_v1', -- Logical target, not an environment's physical path
    @checkpoint_ref = NULL,                        -- Copy has no Spark producer checkpoint
    @capture_mode = N'WATERMARK',                  -- Configure watermark columns/boundaries separately
    @bronze_representation = N'EVENT_LOG',          -- Retain observations; does not prove full source history
    @is_enabled = 1;                              -- Enable future normal planning

-- Procedures resolve FKs; do not supply environment-specific generated configuration IDs.
EXEC metadata.usp_upsert_bronze_to_silver_config
    @dataset_id = N'customer',                     -- Logical Silver dataset name
    @contract_version = 2,                        -- New configuration row for customer v2
    @execution_group_id = N'crm_silver_validation', -- May differ from the producer/v1 execution group
    @source_dataset_id = N'crm_customer',          -- Reference the producer's logical dataset name
    @source_contract_version = 1,                 -- Reuse producer v1 independently of the Silver version
    @silver_relation_ref = N'silver.customer_v2',   -- Separate v2 destination
    @checkpoint_ref = N'checkpoints.customer_silver_v2', -- Stable, independent v2 consumption progress
    @load_strategy = N'SCD1',                      -- Configure key, ordering and delete policies separately
    @is_enabled = 1;
```

An executable domain script also registers the owning `bronze_policy`/`silver_policy` rows and
applicable `bronze_rule`/`silver_rule` checks. Field definitions and the watermark-to-SCD1/SCD2 example
are in [Control-plane policy fields](../architecture/CONTROL_PLANE.md#policy-fields-and-rules).
Apply prerequisite definitions/bindings and policies before requesting a plan. Registry certification
and compatibility checks still apply; these example values do not advertise an implemented reader
or writer. Dev may execute the same SQL manually; UAT/Prod receive reviewed SQL through CI/CD.


## Annotated policy SQL

These planned procedure signatures are illustrative. Required baseline/boundary and hash references
select certified capabilities; they do not replace their proof requirements. List strings below are
validated JSON arrays stored in dedicated fields, not one catch-all policy document. The domain
script declares `@crm_source_columns` and `@customer_target_columns` as full typed schema arrays
before these calls; those declarations are omitted here for readability.

```sql
EXEC metadata.usp_upsert_bronze_policy
    @dataset_id = N'crm_customer',
    @contract_version = 1,                         -- Resolve producer owner, not a policy version
    @policy_schema_version = 1,                    -- Version of the typed policy shape
    @source_fidelity = N'OBSERVATIONS',             -- Current source rows, not an all-change feed
    @record_identity_columns = N'["delivery_id", "customer_id"]', -- Stable delivery-scoped observation
    @ordering_columns = N'["modified_at", "change_id"]', -- Assumes these source fields exist
    @source_boundary_ref = N'frozen_watermark_upper@1',
    @bootstrap_ref = N'full_then_watermark@1',      -- Requires proven baseline-to-increment handoff
    @content_hash_ref = N'canonical_sha256@1',
    @content_columns = N'["customer_id", "name", "modified_at", "change_id"]',
    @schema_ref = N'schemas.crm_customer@1',
    @schema_columns = @crm_source_columns,         -- Full validated typed schema array supplied by domain SQL
    @schema_mode = N'STRICT',
    @watermark_column = N'modified_at',             -- Source extraction cursor role
    @watermark_timezone = N'UTC',
    @watermark_lookback_seconds = 7200;            -- Same row as watermark without lookback
    -- Snapshot and connector-option fields are NULL for this native Copy observation example.

EXEC metadata.usp_upsert_silver_policy
    @dataset_id = N'customer',
    @contract_version = 2,                         -- Resolve the existing SCD1 v2 consumer above
    @policy_schema_version = 1,
    @entity_key_columns = N'["customer_id"]',      -- Silver entity identity, not Bronze record identity
    @ordering_columns = N'["modified_at", "change_id"]',
    @content_hash_ref = N'canonical_sha256@1',
    @content_columns = N'["customer_id", "name"]',
    @schema_ref = N'schemas.customer_current@1',
    @schema_columns = @customer_target_columns,    -- Full expected target schema, not inferred silently
    @schema_mode = N'STRICT',
    @delete_action = N'IGNORE';                    -- This watermark source cannot prove hard deletes
    -- SCD2 and snapshot-publication fields are NULL for SCD1.

EXEC metadata.usp_upsert_silver_rule
    @dataset_id = N'customer',
    @contract_version = 2,
    @rule_id = N'customer_key_not_null',            -- Stable name local to this consumer
    @rule_kind = N'QUALITY',
    @rule_ref = N'not_null@1',                     -- Registered Spark check, not injected Python/SQL
    @rule_order = 10,
    @column_names = N'["customer_id"]',
    @failure_action = N'FAIL';
    -- Comparison/reference fields are NULL for this column-level check.
```

The script also populates Silver v1's own policy and any other required rules before planning.
The examples do not bypass registry certification or advertise implemented procedures.

## Pausing Silver v1 while v2 continues

For the Customer configurations above, the desired state after v2 cutover is:

| Configuration | Logical dataset/version | `is_enabled` |
|---|---|---:|
| Source-to-Bronze | `crm_customer:1` | 1 |
| Bronze-to-Silver v1 | `customer:1` | 0 |
| Bronze-to-Silver v2 | `customer:2` | 1 |

The change uses the public enablement procedure and is retained in desired-state SQL. For a temporary
pause, the engineer later restores the same flag to 1. See
[Enablement and retirement](../architecture/CONTROL_PLANE.md#enablement-and-retirement) for the
canonical pause/resume contract; existing running jobs require an explicit stop action.
