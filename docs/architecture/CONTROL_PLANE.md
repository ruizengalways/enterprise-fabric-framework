---
id: architecture.control-plane
status: current
source_of_truth_for:
  - sql-metadata
  - policy-table-inventory
  - bronze-policy-fields
  - silver-policy-fields
  - pattern-policy-validation
  - runtime-table-fields
  - silver-manifest-fields
  - quality-and-reconciliation-rules
  - frozen-runs
  - checkpoints-and-leases
  - operational-requests
last_reviewed: 2026-09-18
---

# Control plane

This document defines the logical control-plane contract and its default SQL realization. It names
**26 logical tables: 9 metadata tables and 17 operational tables**. They are not implemented yet;
views, Spark checkpoint files and physical binding storage are outside this named-table count. A company
or domain MAY provide another `ControlPlanePort` adapter, but it MUST preserve these logical
ownership, idempotency, fencing and evidence semantics. Concrete SQL types belong to the default
adapter's migrations.

Sample rows below are illustrative, non-runnable examples of table grain and relationships. They
are not seed data, complete schemas or production values. `evidence://...` and `delta://...`
identify bounded evidence, never business rows.


## Table catalog

All logical control-plane tables are maintained in one catalog. The area column describes lifecycle
and ownership only; it is not a separate table model or a separate document.

| `Area` | Table | Row identity / relationship | Why it exists |
|---|---|---|---|
| `metadata` | metadata.execution_group | Logical group ID referenced by producer/consumer configs | Select future jobs by operational ownership; Fabric owns schedules, triggers and retry envelopes. |
| `metadata` | metadata.source_to_bronze_config | Producer dataset/version; generated configuration PK | Configure one independent Source-to-Bronze chain, including destination and executable reader settings; consumers resolve its retained Bronze relation rather than a producer invocation. |
| `metadata` | metadata.bronze_policy | PK/FK -> Source-to-Bronze config | Store source identity/schema and applicable extraction/publication settings. |
| `metadata` | metadata.bronze_dq_rule | Source-to-Bronze config plus stable rule ID | Store repeated source/Bronze data-quality checks. |
| `metadata` | metadata.bronze_recon_rule | Source-to-Bronze config plus stable rule ID | Store repeated source-to-publication reconciliation checks. |
| `metadata` | metadata.bronze_to_silver_config | Consumer dataset/version; FK to a producer config | Reuse Bronze with separate Silver semantics, targets and stable query checkpoints across versions. |
| `metadata` | metadata.silver_policy | PK/FK -> Bronze-to-Silver config | Store target identity/schema/delete settings and applicable load-strategy settings. |
| `metadata` | metadata.silver_dq_rule | Bronze-to-Silver config plus stable rule ID | Store repeated Silver data-quality checks independently for each version. |
| `metadata` | metadata.silver_recon_rule | Bronze-to-Silver config plus stable rule ID | Store repeated Bronze-to-target reconciliation checks independently for each version. |
| `runtime` | control.pipeline_run | Calling Fabric Pipeline/execution-group invocation | Connect planned jobs and evidence to the observable Fabric run. |
| `runtime` | control.bronze_run | One frozen Source-to-Bronze job referencing its config | Preserve resolved settings, boundaries and outcome across metadata edits and retries. |
| `runtime` | control.silver_run | One frozen Bronze-to-Silver invocation referencing its config | Freeze policies/bindings while the query's stable Spark checkpoint outlives individual runs. |
| `runtime` | control.bronze_manifest | Bronze publication/delivery/snapshot evidence | Prove what was published and whether a snapshot is complete; not a consumer scheduling cursor. |
| `runtime` | control.bronze_dq_results | Bronze run plus rule and optional delivery | Store bounded aggregate outcomes for Bronze DQ checks. |
| `runtime` | control.bronze_dq_violation | Bronze DQ result plus stable violation ID | Store bounded references to Bronze row-level DQ failures. |
| `runtime` | control.bronze_recon_violation | Bronze run plus rule and stable violation ID | Store bounded expected-versus-actual source-to-publication mismatches. |
| `runtime` | control.bronze_recon_results | Bronze run and optional delivery | Store aggregate outcomes for Bronze reconciliation rules. |
| `runtime` | control.silver_manifest | One logical Silver micro-batch; completing run FK | Record logical batch counts, commits and reconciliation; preserve each execution attempt and count each committed change once. |
| `runtime` | control.silver_dq_results | Silver run plus rule and optional micro-batch | Store bounded aggregate outcomes for Silver DQ checks. |
| `runtime` | control.silver_dq_violation | Silver DQ result plus stable violation ID | Store bounded references to Silver row-level DQ failures. |
| `runtime` | control.silver_recon_violation | Silver run plus rule and stable violation ID | Store bounded expected-versus-actual input-to-target mismatches. |
| `runtime` | control.silver_recon_results | Silver run and optional micro-batch | Store aggregate outcomes for Silver reconciliation rules. |
| `runtime` | control.source_cursor | Bounded producer config with `checkpoint_ref = NULL` | Persist Source-to-Bronze extraction progress only after proven Bronze publication. |
| `runtime` | control.lease | Resource key and owning worker/run | Fence overlapping query/checkpoint/target mutations and govern shared-provider capacity with compare-and-swap fencing. |
| `runtime` | control.execution_request | One rebuild/recovery request; `operation_type` selects its handler | Share immutable planning, approval, single claim, expiry, retry and terminal evidence across typed operation handlers. |
| `runtime` | control.audit_event | Referenced config/run/request and actor/action | Retain configuration changes, operator/governance actions and retry transitions; reference outcome evidence without duplicating its metrics. |

### Entity relationship diagram

The catalog stays unified; the ER view is split into metadata and runtime diagrams so the
relationships remain readable.

#### Metadata relationships

```mermaid
erDiagram
    direction TB

    ExecutionGroup["metadata.execution_group"] {
        string executionGroupId PK
        string displayName
        string sourceConcurrencyKey
    }
    SourceConfig["metadata.source_to_bronze_config"] {
        string bronzeConfigId PK
        string executionGroupId FK
    }
    BronzePolicy["metadata.bronze_policy"] {
        string bronzeConfigId PK, FK
    }
    BronzeDqRule["metadata.bronze_dq_rule"] {
        string bronzeConfigId PK, FK
        string ruleId PK
    }
    BronzeReconRule["metadata.bronze_recon_rule"] {
        string bronzeConfigId PK, FK
        string ruleId PK
    }
    SilverConfig["metadata.bronze_to_silver_config"] {
        string silverConfigId PK
        string bronzeConfigId FK
        string executionGroupId FK
    }
    SilverPolicy["metadata.silver_policy"] {
        string silverConfigId PK, FK
    }
    SilverDqRule["metadata.silver_dq_rule"] {
        string silverConfigId PK, FK
        string ruleId PK
    }
    SilverReconRule["metadata.silver_recon_rule"] {
        string silverConfigId PK, FK
        string ruleId PK
    }

    ExecutionGroup ||--o{ SourceConfig : groups
    ExecutionGroup ||--o{ SilverConfig : plans
    SourceConfig ||--|| BronzePolicy : owns
    SourceConfig ||--o{ BronzeDqRule : dq_checks
    SourceConfig ||--o{ BronzeReconRule : reconciliation_checks
    SourceConfig ||--o{ SilverConfig : feeds
    SilverConfig ||--|| SilverPolicy : owns
    SilverConfig ||--o{ SilverDqRule : dq_checks
    SilverConfig ||--o{ SilverReconRule : reconciliation_checks
```

#### Runtime relationships

```mermaid
erDiagram
    direction TB

    PipelineRun["control.pipeline_run"] {
        string pipelineRunId PK
        string executionGroupId FK
    }
    BronzeRun["control.bronze_run"] {
        string bronzeRunId PK
        string pipelineRunId FK
        string bronzeConfigId FK
    }
    SilverRun["control.silver_run"] {
        string silverRunId PK
        string pipelineRunId FK
        string silverConfigId FK
    }
    BronzeManifest["control.bronze_manifest"] {
        string deliveryId PK
        string bronzeRunId FK
    }
    BronzeDqResults["control.bronze_dq_results"] {
        string bronzeRunId PK, FK
        string ruleId PK
    }
    BronzeDqViolation["control.bronze_dq_violation"] {
        string bronzeRunId PK, FK
        string ruleId PK
        string violationId PK
    }
    BronzeReconViolation["control.bronze_recon_violation"] {
        string bronzeRunId PK, FK
        string ruleId PK
        string violationId PK
    }
    BronzeReconResults["control.bronze_recon_results"] {
        string bronzeRunId PK, FK
    }
    SilverManifest["control.silver_manifest"] {
        string queryIdentity PK
        int batchId PK
        string silverRunId FK
    }
    SilverDqResults["control.silver_dq_results"] {
        string silverRunId PK, FK
        string ruleId PK
        int batchId PK
    }
    SilverDqViolation["control.silver_dq_violation"] {
        string silverRunId PK, FK
        string ruleId PK
        int batchId PK
        string violationId PK
    }
    SilverReconViolation["control.silver_recon_violation"] {
        string silverRunId PK, FK
        string ruleId PK
        int batchId PK
        string violationId PK
    }
    SilverReconResults["control.silver_recon_results"] {
        string silverRunId PK, FK
        int batchId PK
    }
    SourceCursor["control.source_cursor"] {
        string sourceToBronzeConfigId PK, FK
    }
    Lease["control.lease"] {
        string resourceKey PK
        string ownerRunId
    }
    ExecutionRequest["control.execution_request"] {
        string requestId PK
    }
    AuditEvent["control.audit_event"] {
        string eventId PK
        string runId
        string requestId
    }

    PipelineRun ||--o{ BronzeRun : launches
    PipelineRun ||--o{ SilverRun : launches
    BronzeRun ||--o{ BronzeManifest : publishes
    BronzeRun ||--o{ BronzeDqResults : evaluates
    BronzeDqResults ||--o{ BronzeDqViolation : identifies
    BronzeRun ||--o{ BronzeReconViolation : reconciles
    BronzeRun ||--|| BronzeReconResults : summarizes
    SilverRun ||--o{ SilverManifest : completes
    SilverRun ||--o{ SilverDqResults : evaluates
    SilverDqResults ||--o{ SilverDqViolation : identifies
    SilverRun ||--o{ SilverReconViolation : reconciles
    SilverRun ||--o{ SilverReconResults : summarizes
    ExecutionRequest ||--o| PipelineRun : authorizes
    PipelineRun ||--o{ AuditEvent : records
    ExecutionRequest ||--o{ AuditEvent : records
```

## Metadata model

### execution-group

**Purpose:** Groups producer and consumer configurations for normal planning; scope: one logical
execution group and its optional shared provider-capacity key.

**Description:** A group is a future-planning selection boundary, not a Fabric schedule, Pipeline,
checkpoint or execution algorithm.

| Column | Value | Description |
|---|---|---|
| `execution_group_id` | crm_daily | Required stable logical primary key; passed to planning and stored on the parent Pipeline run. |
| `display_name` | CRM daily ingestion | Required operator-facing name; presentation only and safe to edit. |
| `description` | CRM daily ingestion and ownership | Optional operational purpose and ownership notes; never parsed as executable configuration or a source selector. |
| `source_concurrency_key` | crm_provider | Optional logical provider-capacity key shared by groups that must observe one provider limit through bounded leases. |

```text
PRIMARY KEY (execution_group_id)
```

**Sample rows**

| execution_group_id | display_name | source_concurrency_key |
|---|---|---|
| `crm_daily` | CRM daily ingestion | `crm_provider` |
| `crm_silver_daily` | CRM customer Silver | `NULL` |

### Source to bronze configuration

**Purpose:** Defines an independent Source-to-Bronze producer contract; scope: one producer
dataset/version, destination relation, reader configuration and optional checkpoint.

**Description:** Silver consumers reference the retained Bronze relation through this configuration;
they do not claim a producer invocation or use its configuration ID as a row filter.

| Column | Value | Description |
|---|---|---|
| `source_to_bronze_config_id` | 101 | Generated immutable primary key and internal owner ID for this producer contract. |
| `dataset_id` | crm_customer | Required logical producer identity within the Workspace. |
| `contract_version` | 1 | Required positive producer-facing retained-fact contract version. |
| `execution_group_id` | crm_daily | Required FK to `metadata.execution_group`; selects this configuration for future normal planning only. |
| `source_profile_description` | CRM / dbo.Customer | Optional descriptive text; never executable source configuration and never contains credentials. |
| `source_reader_ref` | delta_sharing_snapshot@1 | Optional registered reader capability; required for package ingress and `NULL` for native Copy/Dataflow. |
| `source_connection_ref` | connections.crm_share | Optional environment-bound connection reference; required only when the selected reader needs one. |
| `bronze_relation_ref` | bronze.crm_customer_v1 | Required logical relation reference resolved to the append-published Bronze relation. |
| `checkpoint_ref` | checkpoints.crm_customer | Required for `STRUCTURED_STREAMING` readers and `NULL` for `BOUNDED` producers; stable across runs and deleted only after permanent decommissioning stops the query and scheduling. |
| `capture_mode` | **WATERMARK** | Bounded incremental extraction using a proven source boundary and cursor. |
|  | **FULL** | Complete source extraction; watermark fields are normally `NULL`. |
|  | **CDC** | Change-feed extraction requiring declared ordering, identity and source fidelity. |
| `bronze_representation` | **EVENT_LOG** | Append-only retained source events or changes. |
|  | **SNAPSHOT** | Complete state for a declared selection; publication requires completeness proof. |
| `is_enabled` | **true** | Configuration may be selected for future normal planning after validation. |
|  | **false** | Excluded from future normal plans; does not cancel active runs or delete history/checkpoints. |

```text
PRIMARY KEY (source_to_bronze_config_id)
FOREIGN KEY (execution_group_id) REFERENCES metadata.execution_group(execution_group_id)
UNIQUE (dataset_id, contract_version)
```

**Sample rows**

| source_to_bronze_config_id | dataset_id | contract_version | execution_group_id | capture_mode | bronze_relation_ref |
|---:|---|---:|---|---|---|
| `101` | `crm_customer` | `1` | `crm_daily` | `WATERMARK` | `bronze.crm_customer_v1` |
| `102` | `crm_country` | `1` | `crm_daily` | `FULL` | `bronze.crm_country_v1` |

### bronze to silver configuration

**Purpose:** Defines a Silver consumer contract over a retained Bronze relation; scope: one consumer
dataset/version, target relation and stable query checkpoint.

**Description:** One logical Silver dataset can have v1 and v2 rows that share a valid
Source-to-Bronze configuration while retaining independent targets and checkpoints.

| Column | Value | Description |
|---|---|---|
| `bronze_to_silver_config_id` | 201 | Generated immutable primary key and internal owner ID for this consumer contract. |
| `dataset_id` | customer | Required logical Silver dataset identity within the Workspace. |
| `contract_version` | 1 | Required positive consumer-facing Silver contract version. |
| `execution_group_id` | crm_silver_daily | Required FK to `metadata.execution_group`; selects this configuration for future normal planning. |
| `source_to_bronze_config_id` | 101 | Required FK to the producer contract whose resolved Bronze relation this Silver version consumes. |
| `silver_relation_ref` | silver.customer_v1 | Required logical relation reference resolved to the consumer target relation. |
| `checkpoint_ref` | checkpoints.customer_silver_v1 | Required stable path unique per environment and query contract; independent across versions and deleted only after permanent decommissioning stops the query and scheduling. |
| `load_strategy` | **APPEND** | Adds accepted events while enforcing the strategy's identity and replay rules. |
|  | **SCD1** | Applies deterministic current-state updates for identified entities. |
|  | **SCD2** | Maintains ordered historical entity versions and effective intervals. |
|  | **REPLACE** | Publishes a validated replacement target from a complete candidate input. |
| `is_enabled` | **true** | Configuration may be selected for future normal planning after validation. |
|  | **false** | Excluded from future normal plans; does not cancel active runs or delete history/checkpoints. |

```text
PRIMARY KEY (bronze_to_silver_config_id)
FOREIGN KEY (execution_group_id) REFERENCES metadata.execution_group(execution_group_id)
FOREIGN KEY (source_to_bronze_config_id) REFERENCES metadata.source_to_bronze_config(source_to_bronze_config_id)
UNIQUE (dataset_id, contract_version)
```

**Sample rows**

| bronze_to_silver_config_id | dataset_id | contract_version | source_to_bronze_config_id | load_strategy | silver_relation_ref |
|---:|---|---:|---:|---|---|
| `201` | `customer` | `1` | `101` | `SCD1` | `silver.customer_v1` |
| `202` | `country` | `1` | `102` | `REPLACE` | `silver.country_v1` |
### Bronze policy

**Purpose:** Declares source identity, schema, capture, boundary and publication policy; scope: one
Source-to-Bronze configuration.

**Description:** Its typed fields select registered source and publication capabilities; irrelevant
or missing fields for the chosen capture pattern fail planning.

| Column | Value | Description |
|---|---|---|
| `source_to_bronze_config_id` | 101 | Required PK/FK to the producer configuration that owns this policy. |
| `policy_schema_version` | 1 | Required positive version of the policy-field contract, independent of dataset contract version. |
| `source_object_ref` | crm.dbo.Customer | Optional logical reader table or endpoint; `NULL` when Fabric Copy owns physical source selection. |
| `source_fidelity` | **OBSERVATIONS** | Reader exposes observed source facts without a complete-state or change-feed guarantee. |
|  | **COMPLETE_STATE** | Reader exposes complete state for the declared source selection. |
|  | **ALL_CHANGES** | Reader exposes the declared source change history. |
|  | **IMMUTABLE_EVENTS** | Reader exposes immutable, independently identifiable events. |
| `record_identity_columns` | ["delivery_id", "customer_id"] | Required retained-record identity list used for replay and conflict checks. |
| `ordering_columns` | ["lsn", "event_id"] | Required source-position and tie-breaker list for deterministic ordering. |
| `source_operation_column` | operation | Optional retained source-action column; required when source operations are declared facts. |
| `source_boundary_ref` | frozen_watermark_upper@1 | Optional registered capability that selects and proves an extraction upper boundary. |
| `bootstrap_ref` | full_then_watermark@1 | Optional registered baseline/handoff capability; required for an initial baseline before incremental capture. |
| `content_hash_ref` | canonical_sha256@1 | Optional canonical comparison capability; required when content equality or conflict detection is selected. |
| `content_columns` | ["customer_id", "name", "modified_at"] | Optional compared business columns; excludes technical delivery columns. |
| `schema_ref` | schemas.crm_customer@1 | Required expected source-schema identity. |
| `schema_columns` | [{"name":"customer_id","type":"long","nullable":false}] | Required typed schema used to validate the resolved reader schema. |
| `schema_mode` | **STRICT** | Only the declared schema shape is accepted. |
|  | **ADDITIVE_NULLABLE** | Bronze-only reviewed mode: declared columns remain compatible and additional nullable source columns are accepted. |
| `watermark_column` | modified_at | Optional increment-predicate column; required for a temporal `WATERMARK` capability. |
| `watermark_tie_breaker_columns` | ["change_id"] | Optional composite-cursor tie breakers; required when the watermark column is not unique. |
| `watermark_timezone` | UTC | Optional timezone for temporal boundaries; `NULL` for sequence or non-temporal cursors. |
| `watermark_lookback_seconds` | 7200 | Required non-negative temporal overlap; `0` does not move the committed cursor backwards. |
| `snapshot_identity_column` | capture_id | Optional delivery identity; required when a snapshot uses a source or provider token. |
| `snapshot_scope_columns` | ["region"] | Optional snapshot-scope keys; `[]` means the complete selection has one scope. |
| `snapshot_completeness_ref` | provider_snapshot_token@1 | Optional registered completeness-proof capability; required before `SNAPSHOT` publication completes. |
| `reader_options_schema_ref` | options.api_paging@1 | Optional versioned schema defining permitted connector-option names and types. |
| `reader_options` | {"page_size": 1000} | Optional validated bounded connector options; never credentials or executable code. |

```text
PRIMARY KEY (source_to_bronze_config_id)
FOREIGN KEY (source_to_bronze_config_id) REFERENCES metadata.source_to_bronze_config(source_to_bronze_config_id)
```

Changing `schema_mode`, `schema_ref` or `schema_columns` through the public Bronze-policy upsert
changes only future planning; each Bronze run retains its frozen policy. To disable
`ADDITIVE_NULLABLE`, the desired-state update MUST set `schema_ref` and `schema_columns` to the
latest approved accepted Bronze schema and set `schema_mode = STRICT` in the same update. It MUST
NOT drop retained Bronze columns or rewrite prior deliveries.

**Sample rows**

| source_to_bronze_config_id | source_fidelity | record_identity_columns | schema_mode | watermark_column |
|---:|---|---|---|---|
| `101` | `OBSERVATIONS` | `[delivery_id, customer_id]` | `STRICT` | `modified_at` |
| `102` | `COMPLETE_STATE` | `[capture_id, country_code]` | `STRICT` | `NULL` |

### Silver policy

**Purpose:** Declares target schema, mutation, delete, history and snapshot-consumption policy;
scope: one Bronze-to-Silver configuration.

**Description:** Common identity, ordering, schema and delete settings serve several strategies;
SCD2 and snapshot publication use conditional fields instead of separate policy tables.

| Column | Value | Description |
|---|---|---|
| `bronze_to_silver_config_id` | 201 | Required PK/FK to the consumer configuration that owns this policy. |
| `policy_schema_version` | 1 | Required positive version of the policy-field contract, independent of dataset contract version. |
| `entity_key_columns` | ["customer_id"] | Optional ordered entity keys; required for keyed strategies such as `UPSERT`, `SCD1` and `SCD2`. |
| `event_identity_columns` | ["event_id"] | Optional APPEND event identity; required for append conflict and replay protection. |
| `ordering_columns` | ["source_version", "event_id"] | Required deterministic winner and ordering columns, including all tie breakers. |
| `content_hash_ref` | canonical_sha256@1 | Optional canonical comparison capability; required when change detection compares content. |
| `content_columns` | ["customer_id", "name"] | Optional business columns included in content equality. |
| `schema_ref` | schemas.customer@1 | Required expected Silver-schema identity for the consumer-facing contract. |
| `schema_columns` | [{"name":"customer_id","type":"long","nullable":false}] | Required typed schema used to validate projected target data. |
| `schema_mode` | **STRICT** | Only the declared target schema shape is accepted. |
| `source_operation_column` | operation | Optional supplied input action column. |
| `delete_operation_values` | ["D"] | Optional typed source action values interpreted as deletes by the registered capability. |
| `delete_marker_column` | is_deleted | Optional input marker used when a boolean or status represents deletion. |
| `delete_marker_values` | [true] | Optional typed marker values; values are not parsed from text. |
| `delete_action` | IGNORE | Declared delete facts do not mutate the target. |
|  | HARD_DELETE | Declared delete facts remove matching target entities. |
|  | SCD2_CLOSE | Declared delete facts close the active SCD2 interval. |
| `effective_time_column` | effective_at | Optional history time; required for strategies that maintain effective intervals. |
| `tracked_columns` | ["name", "status"] | Optional change-tracked columns that create a new history version. |
| `late_arrival_policy` | REJECT_AND_REBUILD | Late data is rejected and requires a governed rebuild to correct history. |
|  | CORRECT_WITHIN_WINDOW | Late data may correct history within `correction_window_seconds`. |
| `correction_window_seconds` | 604800 | Required non-negative correction window for `CORRECT_WITHIN_WINDOW`; otherwise `NULL`. |
| `snapshot_selection_ref` | latest_complete_snapshot@1 | Optional complete-snapshot selection capability; required for snapshot-diff consumers. |
| `snapshot_diff_apply_ref` | snapshot_diff_to_scd1@1 | Optional capability that converts snapshot differences to mutations. |
| `replace_publication_ref` | validated_stable_target@1 | Optional candidate-cutover capability; required for `REPLACE` publication. |

```text
PRIMARY KEY (bronze_to_silver_config_id)
FOREIGN KEY (bronze_to_silver_config_id) REFERENCES metadata.bronze_to_silver_config(bronze_to_silver_config_id)
```

`metadata.silver_policy` MUST use `STRICT`; `ADDITIVE_NULLABLE` is a Bronze-only source-schema
mode. A Silver schema change of any kind requires a new `bronze_to_silver_config` contract version
with its own target relation and checkpoint. The existing Silver version MUST NOT evolve in place.

**Sample rows**

| bronze_to_silver_config_id | entity_key_columns | ordering_columns | delete_action | schema_mode |
|---:|---|---|---|---|
| `201` | `[customer_id]` | `[modified_at, change_id]` | `IGNORE` | `STRICT` |
| `202` | `[country_code]` | `[]` | `IGNORE` | `STRICT` |

Delete values are typed arrays, not text that the executor guesses how to interpret. When using
both operation and marker evidence, the capability MUST declare precedence/conflict behavior.
Setting HARD_DELETE without supported delete facts MUST fail planning. Source operation columns in
the two stage policies may name the same field, but only Silver declares target delete behavior.

`ordering_columns` includes all required tie breakers; `effective_time_column` retains its distinct
history-interval role even when it is also an ordering column. Snapshot selection capabilities MUST
use complete, comparable declared scopes. REPLACE publication guards and APPEND conflict/replay
behavior remain those of the selected registered strategy.

### Bronze DQ rules

**Purpose:** Registers producer-side data-quality checks; scope: one
Source-to-Bronze configuration and its stable rule IDs.

**Description:** Checks use child rows rather than a growing set of columns or a JSON document.
Bronze and Silver rules have separate owning foreign keys, so no row couples Silver versions.

| Column | Value | Description |
|---|---|---|
| `source_to_bronze_config_id` | 101 | Required FK to the producer configuration that owns this rule. |
| `rule_id` | copied_count_matches_source | Required stable local rule ID, unique within its owning configuration. |
| `rule_ref` | row_count_matches@1 | Required registered check capability and version. |
| `rule_order` | 10 | Required non-negative order within the registered execution phase. |
| `column_names` | ["customer_id"] | Optional validated input columns; use `[]` when the check consumes only aggregate or reference metrics. |
| `reference_relation_ref` | bronze.customer_baseline | Optional declared comparison relation; required when the rule compares against a relation. |
| `reference_metric_ref` | source.selected_rows | Optional bounded evidence metric; required when the rule compares a recorded metric. |
| `comparison` | **EQ** | Equal-to comparison for threshold-based checks. |
|  | **GE** | Greater-than-or-equal comparison for threshold-based checks. |
|  | **LE** | Less-than-or-equal comparison for threshold-based checks. |
| `threshold` | 0 | Optional numeric threshold interpreted only by the registered rule capability. |
| `severity` | **CRITICAL** | Required impact classification for a failed rule. `CRITICAL` can block its table delivery; `WARNING` records bounded evidence but does not block other tables. |
| `failure_action` | **FAIL** | Stops the governed operation when the rule fails. |
|  | **REPORT** | Records the failure without stopping when the rule contract permits it. |


Each check's registry contract defines its execution phase, required fields, valid comparator,
evidence inputs and permitted severity/failure-action combination. `FAIL` requires `CRITICAL`
severity; `WARNING` rules use `REPORT`. Rules run over Spark/Delta and return bounded metrics
and evidence references. No rule contains arbitrary Python, a domain callback or unrestricted SQL.
Selected columns/references MUST exist and be declared in the frozen plan. Irrelevant or missing
fields fail validation. Checks requiring unsupported additional parameters fail planning until the
typed model and registered capability support them; do not add arbitrary parameter bags as a workaround.

Bronze validation MUST NOT silently discard source facts and still declare a complete snapshot.
Bronze DQ rules MUST NOT use `QUARANTINE`. Silver `QUARANTINE` is for supported row-level quality checks, not a way to ignore failed final
reconciliation. Required completeness, replay/conflict and strategy invariants cannot be changed to
REPORT or omitted by removing optional rule rows.

**Sample rows**

| source_to_bronze_config_id | rule_id | rule_ref | column_names | failure_action |
|---:|---|---|---|---|
| `101` | `customer_id_not_null` | `not_null@1` | `[customer_id]` | `FAIL` |
| `102` | `country_code_not_null` | `not_null@1` | `[country_code]` | `FAIL` |

### Bronze reconciliation rules

**Purpose:** Registers producer-side source-to-publication evidence comparisons; scope: one
Source-to-Bronze configuration and its stable rule IDs.

The table uses the same owner, stable `rule_id`, registered `rule_ref`, phase, ordering, severity
and typed comparison fields as Bronze DQ rules. Its capability compares bounded source evidence selected by
`reference_metric_ref` or `reference_relation_ref` with Bronze publication evidence. Its
`failure_action` is only `FAIL` or `REPORT`; reconciliation never quarantines records.

```text
PRIMARY KEY (source_to_bronze_config_id, rule_id)
FOREIGN KEY (source_to_bronze_config_id) REFERENCES metadata.source_to_bronze_config(source_to_bronze_config_id)
```

**Sample rows**

| source_to_bronze_config_id | rule_id | rule_ref | reference_metric_ref | failure_action |
|---:|---|---|---|---|
| `101` | `selected_matches_published` | `row_count_matches@1` | `source.selected_rows` | `FAIL` |
| `102` | `snapshot_manifest_matches` | `file_manifest_matches@1` | `source.file_manifest` | `FAIL` |

### Silver DQ rules

**Purpose:** Registers consumer-side data-quality checks; scope: one
Bronze-to-Silver configuration and its stable rule IDs.

**Description:** The consumer owns these checks independently of other Silver versions, even when
they consume the same Source-to-Bronze configuration.

| Column | Value | Description |
|---|---|---|
| `bronze_to_silver_config_id` | 201 | Required FK to the consumer configuration that owns this rule. |
| `rule_id` | customer_key_not_null | Required stable local rule ID, unique within its owning configuration. |
| `rule_ref` | not_null@1 | Required registered check capability and version. |
| `rule_order` | 10 | Required non-negative order within the registered execution phase. |
| `column_names` | ["customer_id"] | Optional validated input columns; required when the check operates on named columns. |
| `reference_relation_ref` | silver.customer_previous | Optional declared comparison relation; required when the rule compares against a relation. |
| `reference_metric_ref` | batch.accepted_rows | Optional bounded evidence metric; required when the rule compares a recorded metric. |
| `comparison` | **EQ**, **GE** or **LE** | Optional typed comparator required by threshold-based checks. |
| `threshold` | 0 | Optional numeric threshold interpreted only by the registered rule capability. |
| `severity` | **CRITICAL** | Required impact classification for a failed rule. `CRITICAL` can block its table delivery; `WARNING` records bounded evidence but does not block other tables. |
| `failure_action` | **FAIL** | Stops the governed operation when the rule fails. |
|  | **REPORT** | Records the failure without stopping when the rule contract permits it. |
|  | **QUARANTINE** | Diverts failing rows only for supported row-level quality checks. |

```text
PRIMARY KEY (bronze_to_silver_config_id, rule_id)
FOREIGN KEY (bronze_to_silver_config_id) REFERENCES metadata.bronze_to_silver_config(bronze_to_silver_config_id)
```

**Sample rows**

| bronze_to_silver_config_id | rule_id | rule_ref | column_names | failure_action |
|---:|---|---|---|---|
| `201` | `customer_key_not_null` | `not_null@1` | `[customer_id]` | `QUARANTINE` |
| `202` | `country_name_not_null` | `not_null@1` | `[country_name]` | `FAIL` |

### Silver reconciliation rules

**Purpose:** Registers consumer-side Bronze-to-target evidence comparisons; scope: one
Bronze-to-Silver configuration and its stable rule IDs.

The table uses the same owner, stable `rule_id`, registered `rule_ref`, phase, ordering, severity and typed
comparison fields as Silver DQ rules. Its capability compares bounded Bronze input and target
mutation evidence. Its `failure_action` is only `FAIL` or `REPORT`; reconciliation never
quarantines records.

```text
PRIMARY KEY (bronze_to_silver_config_id, rule_id)
FOREIGN KEY (bronze_to_silver_config_id) REFERENCES metadata.bronze_to_silver_config(bronze_to_silver_config_id)
```

**Sample rows**

| bronze_to_silver_config_id | rule_id | rule_ref | reference_metric_ref | failure_action |
|---:|---|---|---|---|
| `201` | `accepted_matches_mutations` | `mutation_count_matches@1` | `batch.accepted_rows` | `FAIL` |
| `202` | `replace_count_matches_candidate` | `row_count_matches@1` | `candidate.accepted_rows` | `FAIL` |


One Source-to-Bronze configuration is reusable by zero or more Bronze-to-Silver consumers. The
consumer row owns its own Silver policy and rules, while its
`source_to_bronze_config_id` points back to the shared producer:

```text
source_to_bronze_config 101
├── bronze_policy 101
├── bronze_dq_rule (101, rule_id), zero or more
├── bronze_recon_rule (101, rule_id), zero or more
├── bronze_to_silver_config 201
│   ├── silver_policy 201
│   ├── silver_dq_rule (201, rule_id), zero or more
│   └── silver_recon_rule (201, rule_id), zero or more
└── bronze_to_silver_config 202
    ├── silver_policy 202
    ├── silver_dq_rule (202, rule_id), zero or more
    └── silver_recon_rule (202, rule_id), zero or more
```

Both consumer rows contain `source_to_bronze_config_id = 101`. The Silver policy and rule tables do
not repeat that producer ID: their owner is the `bronze_to_silver_config_id`, whose row already
links to the producer. Therefore the Silver rule identity is
`(bronze_to_silver_config_id, rule_id)`, not `(source_to_bronze_config_id, rule_id)`. A Silver plan
still resolves both sides and carries the consumer config/policy plus the producer config/policy.



## Runtime table contracts

Runtime rows use opaque IDs and references. They record plans, lifecycle, bounded evidence and
coordination state; business rows, Spark offsets, Spark state and detailed row-level diagnostics stay
in their authoritative Spark/Delta locations. The logical shapes below define the fields and
relationships required by the default control plane.

### pipeline run

**Purpose:** Tracks one planned control-plane and Fabric invocation through its lifecycle; scope:
one execution-group run.

**Description:** It is the parent record that correlates a requested group, frozen plan, Fabric run
and bounded outcome evidence.

| Column | Value | Description |
|---|---|---|
| `pipeline_run_id` | run_01 | Required immutable primary key for one planning and invocation lifecycle. |
| `execution_group_id` | crm_daily | Required FK to the requested execution group. |
| `environment_ref` | prod | Required deployment environment and Workspace-bound runtime scope. |
| `fabric_pipeline_run_id` | fabric_123 | Optional external Fabric Pipeline run identity for monitoring correlation. |
| `execution_request_id` | request_01 | Optional FK to the approved one-time operation that caused this invocation. |
| `plan_hash` | sha256:... | Required frozen identity of selected configs, policies, rules and bindings. |
| `status` | **PLANNED** | Frozen plan exists; Pipeline work has not started. |
|  | **RUNNING** | Pipeline execution has started. |
|  | **SUCCEEDED** | Required work and outcome evidence completed. |
|  | **SUCCEEDED_WITH_WARNINGS** | Every table completed, with only non-blocking warning rule failures. |
|  | **PARTIAL_FAILURE** | At least one independent table failed while other planned tables completed or were attempted. |
|  | **FAILED** | Execution ended with bounded failure evidence. |
|  | **CANCELLED** | Execution was deliberately stopped. |
| `requested_at` | 2026-09-18T10:00:00Z | Required planning acceptance time. |
| `started_at` | 2026-09-18T10:01:00Z | Optional execution start time; `NULL` before work begins. |
| `completed_at` | 2026-09-18T10:10:00Z | Required for terminal statuses and `NULL` while active. |
| `error_code` | SOURCE_UNAVAILABLE | Optional bounded terminal failure category; never a stack trace. |
| `evidence_ref` | evidence://run_01 | Optional bounded reference to detailed diagnostics. |

```text
PRIMARY KEY (pipeline_run_id)
FOREIGN KEY (execution_group_id) REFERENCES metadata.execution_group(execution_group_id)
FOREIGN KEY (execution_request_id) REFERENCES control.execution_request(execution_request_id)
```

**Sample rows**

| pipeline_run_id | execution_group_id | environment_ref | status | plan_hash |
|---|---|---|---|---|
| `pipe_20260921_01` | `crm_daily` | `dev` | `SUCCEEDED` | `sha256:plan-a1` |
| `pipe_20260921_02` | `crm_silver_daily` | `dev` | `RUNNING` | `sha256:plan-b2` |

### bronze run

**Purpose:** Records one frozen Source-to-Bronze execution attempt; scope: one producer
configuration and parent pipeline run.

**Description:** Retries create new attempt rows, preserving the resolved producer configuration,
source boundary and execution mode that governed each attempt.

| Column | Value | Description |
|---|---|---|
| `bronze_run_id` | bronze_run_01 | Required immutable primary key passed to the Source-to-Bronze runner. |
| `pipeline_run_id` | run_01 | Required FK to the parent group invocation. |
| `source_to_bronze_config_id` | 101 | Required FK to the producer configuration frozen for this run. |
| `execution_request_id` | request_01 | Optional FK to an approved one-time operation. |
| `attempt_number` | 2 | Required positive producer attempt number; retries create a new row. |
| `source_execution_mode` | **BOUNDED** | Reads a bounded source; `checkpoint_ref` is `NULL`. |
|  | **STRUCTURED_STREAMING** | Runs a streaming reader; `checkpoint_ref` is required. |
| `bronze_relation_ref` | bronze.crm_customer_v1 | Required frozen Bronze target relation. |
| `checkpoint_ref` | checkpoints.crm_customer | Optional frozen producer checkpoint; required only for `STRUCTURED_STREAMING`. |
| `source_boundary_ref` | boundary://crm/42 | Optional frozen source boundary required by bounded capture capabilities. |
| `plan_hash` | sha256:... | Required frozen producer-plan identity. |
| `status` | **PLANNED** | Frozen plan exists; execution has not started. |
|  | **RUNNING** | Runner has claimed and started the job. |
|  | **SUCCEEDED** | Required publication and evidence completed. |
|  | **FAILED** | Execution ended with bounded failure evidence. |
|  | **CANCELLED** | Execution was deliberately stopped. |
| `started_at` | 2026-09-18T10:01:00Z | Optional start time; `NULL` until claimed. |
| `completed_at` | 2026-09-18T10:10:00Z | Required for terminal statuses and `NULL` while active. |
| `error_code` | SOURCE_UNAVAILABLE | Optional bounded failed-stage category. |
| `evidence_ref` | evidence://bronze_run_01 | Optional reference to Bronze manifest or detailed Delta evidence. |

```text
PRIMARY KEY (bronze_run_id)
FOREIGN KEY (pipeline_run_id) REFERENCES control.pipeline_run(pipeline_run_id)
FOREIGN KEY (source_to_bronze_config_id) REFERENCES metadata.source_to_bronze_config(source_to_bronze_config_id)
FOREIGN KEY (execution_request_id) REFERENCES control.execution_request(execution_request_id)
```

**Sample rows**

| bronze_run_id | pipeline_run_id | source_to_bronze_config_id | attempt_number | status | source_boundary_ref |
|---|---|---:|---:|---|---|
| `bronze_20260921_01` | `pipe_20260921_01` | `101` | `1` | `SUCCEEDED` | `boundary://crm/2026-09-21T00:00Z` |
| `bronze_20260921_02` | `pipe_20260921_01` | `102` | `1` | `SUCCEEDED` | `snapshot://crm-country/2026-09-21` |
### Silver Run

**Purpose:** Records one frozen Silver query execution attempt; scope: one consumer configuration,
checkpoint contract and parent pipeline run.

**Description:** Each retry creates a new run row while retaining the consumer contract's stable
`query_identity` and `checkpoint_ref`. This table records the attempt lifecycle and frozen plan;
`control.silver_manifest` records the idempotent effects and completion evidence for its
micro-batches.

| Column | Value | Description |
|---|---|---|
| `silver_run_id` | silver_run_01 | Required immutable primary key passed to the Silver Spark runner. |
| `pipeline_run_id` | run_01 | Required FK to the parent group invocation. |
| `bronze_to_silver_config_id` | 201 | Required FK to the consumer configuration frozen for this run. |
| `source_to_bronze_config_id` | 101 | Required FK to the resolved producer retained for evidence and diagnosis. |
| `execution_request_id` | request_01 | Optional FK to an approved one-time operation. |
| `attempt_number` | 2 | Required positive query attempt number; retries create new run rows. |
| `query_identity` | customer_v1_generation_1 | Required stable checkpoint-generation identity; changes only for an explicit fresh checkpoint or bootstrap plan. |
| `silver_relation_ref` | silver.customer_v1 | Required frozen Silver target relation. |
| `checkpoint_ref` | checkpoints.customer_silver_v1 | Required frozen checkpoint owned by the consumer contract, never mutable SQL progress. |
| `plan_hash` | sha256:... | Required frozen consumer-plan identity. |
| `status` | **PLANNED** | Frozen plan exists; execution has not started. |
|  | **RUNNING** | Runner has claimed and started the query. |
|  | **SUCCEEDED** | Required processing and evidence completed. |
|  | **FAILED** | Execution ended with bounded failure evidence. |
|  | **CANCELLED** | Execution was deliberately stopped. |
| `started_at` | 2026-09-18T10:01:00Z | Optional start time; `NULL` until the runner claims the query. |
| `completed_at` | 2026-09-18T10:10:00Z | Required for terminal statuses and `NULL` while active. |
| `error_code` | TARGET_WRITE_FAILED | Optional bounded failed-stage category. |
| `evidence_ref` | evidence://silver_run_01 | Optional reference to Silver manifests or detailed Delta evidence. |

```text
PRIMARY KEY (silver_run_id)
FOREIGN KEY (pipeline_run_id) REFERENCES control.pipeline_run(pipeline_run_id)
FOREIGN KEY (bronze_to_silver_config_id) REFERENCES metadata.bronze_to_silver_config(bronze_to_silver_config_id)
FOREIGN KEY (source_to_bronze_config_id) REFERENCES metadata.source_to_bronze_config(source_to_bronze_config_id)
FOREIGN KEY (execution_request_id) REFERENCES control.execution_request(execution_request_id)
```

**Sample rows**

| silver_run_id | pipeline_run_id | bronze_to_silver_config_id | attempt_number | query_identity | status |
|---|---|---:|---:|---|---|
| `silver_20260921_01` | `pipe_20260921_02` | `201` | `1` | `customer_v1_generation_1` | `RUNNING` |
| `silver_20260921_02` | `pipe_20260921_02` | `202` | `1` | `country_v1_generation_1` | `SUCCEEDED` |


### Bronze manifests

**Purpose:** Preserves bounded Bronze publication and snapshot-completeness evidence; scope: one
delivery or complete snapshot publication.

**Description:** The manifest contains bounded metadata only:

- Source-to-Bronze configuration, Bronze/provider run and delivery/snapshot identities;
- completion state;
- landed relation, Delta version, partition or immutable file references;
- frozen source boundary and Bronze representation;
- source schema/contract identity;
- available bounded source/copied/rejected counts; and
- completion time and evidence references.

A provider activity marked successful is insufficient to prove snapshot completeness. The
registered snapshot publication/consumption capability MUST validate its manifest and must handle
publication recovery and snapshots spanning multiple micro-batches. Append-log readers consume
published Delta commits without a per-capture SQL claim.

| Column | Value | Description |
|---|---|---|
| `delivery_id` | delivery_01 | Required immutable primary key for one Bronze delivery or complete snapshot publication. |
| `bronze_run_id` | bronze_run_01 | Required FK to the producer execution that published or attempted this delivery. |
| `source_to_bronze_config_id` | 101 | Required FK to the producer contract that owns the delivery. |
| `bronze_relation_ref` | bronze.crm_customer_v1 | Required relation containing the published delivery. |
| `completion_state` | **PENDING** | Publication exists but completion has not been proven. |
|  | **COMPLETE** | Selected completeness proof and required publication evidence are present. |
|  | **FAILED** | Publication attempt ended without completing the delivery. |
| `delta_version` | 42 | Optional published Delta version for the delivery commit. |
| `source_boundary_ref` | boundary://crm/42 | Optional frozen source interval or snapshot boundary. |
| `snapshot_identity_ref` | snapshot://crm/2026-09-18 | Optional provider or source snapshot identity; required when completeness depends on an external token. |
| `selected_rows` | 1000 | Optional bounded source-selection count. |
| `published_rows` | 998 | Optional bounded Bronze-publication count. |
| `rejected_rows` | 2 | Optional bounded rejected-row count. |
| `evidence_ref` | evidence://delivery_01 | Optional bounded publication or completeness evidence reference. |
| `completed_at` | 2026-09-18T10:10:00Z | Required when `completion_state` is `COMPLETE`. |

```text
PRIMARY KEY (delivery_id)
FOREIGN KEY (bronze_run_id) REFERENCES control.bronze_run(bronze_run_id)
FOREIGN KEY (source_to_bronze_config_id) REFERENCES metadata.source_to_bronze_config(source_to_bronze_config_id)
```

**Sample rows**

| delivery_id | bronze_run_id | completion_state | selected_rows | published_rows | delta_version |
|---|---|---|---:|---:|---:|
| `delivery_customer_20260921` | `bronze_20260921_01` | `COMPLETE` | `1,000` | `1,000` | `42` |
| `delivery_country_20260921` | `bronze_20260921_02` | `COMPLETE` | `195` | `195` | `18` |

### Silver manifests

**Purpose:** Records Silver micro-batch effects, reconciliation and completion evidence; scope: one
query identity and micro-batch ID.

**Description:** Each row is the idempotent logical record for `(query_identity, batch_id)`, even when a batch is
retried. `COMMITTED` proves the target mutation effects; `COMPLETE` additionally proves all
required reconciliation and completion evidence.

| Column | Value | Description |
|---|---|---|
| `query_identity` | customer_v1_generation_1 | Required stable checkpoint-generation identity for one Silver query contract in the environment. |
| `batch_id` | 42 | Required non-negative Spark micro-batch ID, unique with `query_identity`. |
| `silver_run_id` | silver_run_01 | Optional FK to the completing run; `NULL` until the batch is `COMPLETE`. |
| `input_rows` | 1000 | Optional bounded count observed by the batch. |
| `accepted_rows` | 995 | Optional bounded count admitted to the mutation path. |
| `quarantined_rows` | 5 | Optional bounded count diverted by supported row-level quality handling. |
| `inserted_rows` | 700 | Optional target insertion operations, not distinct business entities. |
| `updated_rows` | 295 | Optional target update operations, not distinct business entities. |
| `deleted_rows` | 0 | Optional target deletion operations, not distinct business entities. |
| `target_commit_ref` | delta://silver/customer#42 | Optional committed-operation evidence; required before `COMPLETE` when a target mutation occurred. |
| `reconciliation_ref` | evidence://reconciliation/42 | Optional persisted reconciliation evidence; required before `COMPLETE` when reconciliation is selected. |
| `completion_state` | **PENDING** | Logical batch identity exists; effects are not yet proven. |
|  | **COMMITTED** | Target mutation effects are proven. |
|  | **COMPLETE** | Required reconciliation and completion evidence are proven. |
| `completed_at` | 2026-09-18T10:10:00Z | Required only when `completion_state` is `COMPLETE`. |

```text
PRIMARY KEY (query_identity, batch_id)
FOREIGN KEY (silver_run_id) REFERENCES control.silver_run(silver_run_id)
```

**Sample rows**

| query_identity | batch_id | silver_run_id | input_rows | accepted_rows | quarantined_rows | completion_state |
|---|---:|---|---:|---:|---:|---|
| `customer_v1_generation_1` | `42` | `silver_20260921_01` | `1,000` | `998` | `2` | `COMPLETE` |
| `country_v1_generation_1` | `7` | `silver_20260921_02` | `195` | `195` | `0` | `COMPLETE` |

### Bronze DQ results

**Purpose:** Records the bounded aggregate result of one Bronze DQ rule for a run and optional
delivery. It stores counts and evidence references, never business rows.

| Column | Description |
|---|---|
| `bronze_run_id`, `rule_id` | Required composite identity; `rule_id` resolves to the frozen Bronze DQ rule. |
| `delivery_id` | Optional FK to the delivery evaluated by the rule. |
| `status` | `PENDING`, `PASSED` or `FAILED`. |
| `passed_rows`, `failed_rows` | Optional bounded aggregate counts. |
| `metrics`, `evidence_ref` | Optional bounded typed metrics and Delta evidence reference. |

```text
PRIMARY KEY (bronze_run_id, rule_id)
FOREIGN KEY (bronze_run_id) REFERENCES control.bronze_run(bronze_run_id)
FOREIGN KEY (delivery_id) REFERENCES control.bronze_manifest(delivery_id)
```

**Sample rows**

| bronze_run_id | rule_id | delivery_id | status | passed_rows | failed_rows |
|---|---|---|---|---:|---:|
| `bronze_20260921_01` | `customer_id_not_null` | `delivery_customer_20260921` | `PASSED` | `1,000` | `0` |
| `bronze_20260921_02` | `country_code_not_null` | `delivery_country_20260921` | `PASSED` | `195` | `0` |

### Bronze DQ violations

**Purpose:** Records a bounded reference to one row-level Bronze DQ failure. The failed row or
sample remains in a governed Delta evidence relation named by `evidence_ref`.

| Column | Description |
|---|---|
| `bronze_run_id`, `rule_id`, `violation_id` | Required composite identity; `violation_id` is stable for retry idempotency. |
| `record_identity_ref`, `violation_code` | Optional record identity reference and required registered failure reason. |
| `evidence_ref` | Optional reference to detailed governed evidence. |

```text
PRIMARY KEY (bronze_run_id, rule_id, violation_id)
FOREIGN KEY (bronze_run_id, rule_id) REFERENCES control.bronze_dq_results(bronze_run_id, rule_id)
```

**Sample rows**

| bronze_run_id | rule_id | violation_id | violation_code | record_identity_ref |
|---|---|---|---|---|
| `bronze_20260920_01` | `customer_id_not_null` | `v-001` | `NULL_VALUE` | `record://delivery-customer-20260920/417` |
| `bronze_20260920_01` | `customer_id_not_null` | `v-002` | `NULL_VALUE` | `record://delivery-customer-20260920/982` |

### Bronze reconciliation violations

**Purpose:** Records one bounded source-to-publication reconciliation mismatch; it does not
quarantine or replace the source evidence.

| Column | Description |
|---|---|
| `bronze_run_id`, `rule_id`, `violation_id` | Required composite identity; `rule_id` resolves to the frozen Bronze reconciliation rule. |
| `expected_value`, `actual_value` | Optional canonical bounded comparison values. |
| `evidence_ref` | Optional detailed reconciliation evidence reference. |

```text
PRIMARY KEY (bronze_run_id, rule_id, violation_id)
FOREIGN KEY (bronze_run_id) REFERENCES control.bronze_run(bronze_run_id)
```

**Sample rows**

| bronze_run_id | rule_id | violation_id | expected_value | actual_value |
|---|---|---|---|---|
| `bronze_20260920_02` | `selected_matches_published` | `rv-001` | `1000` | `998` |
| `bronze_20260920_03` | `snapshot_manifest_matches` | `rv-002` | `12 files` | `11 files` |

### Bronze reconciliation results

**Purpose:** Records the aggregate outcome of Bronze reconciliation processing after required
publication and DQ processing.

| Column | Description |
|---|---|
| `bronze_run_id` | Required primary key and FK to the producing run. |
| `delivery_id` | Optional published delivery FK. |
| `status` | `PENDING`, `PASSED` or `FAILED`. |
| `metrics`, `evidence_ref` | Optional bounded reconciliation metrics and aggregate evidence reference. |

```text
PRIMARY KEY (bronze_run_id)
FOREIGN KEY (bronze_run_id) REFERENCES control.bronze_run(bronze_run_id)
FOREIGN KEY (delivery_id) REFERENCES control.bronze_manifest(delivery_id)
```

**Sample rows**

| bronze_run_id | delivery_id | status | metrics | evidence_ref |
|---|---|---|---|---|
| `bronze_20260921_01` | `delivery_customer_20260921` | `PASSED` | `{selected_rows:1000, published_rows:1000}` | `evidence://bronze-recon/01` |
| `bronze_20260921_02` | `delivery_country_20260921` | `PASSED` | `{expected_files:12, published_files:12}` | `evidence://bronze-recon/02` |

### Silver DQ results

**Purpose:** Records the bounded aggregate result of one Silver DQ rule for a run and optional
micro-batch. A rule result is idempotent for its run, rule and batch.

| Column | Description |
|---|---|
| `silver_run_id`, `rule_id`, `batch_id` | Required composite identity; `rule_id` resolves to the frozen Silver DQ rule. |
| `status`, `passed_rows`, `failed_rows` | Required outcome and optional bounded aggregate counts. |
| `metrics`, `evidence_ref` | Optional bounded typed metrics and Delta evidence reference. |

```text
PRIMARY KEY (silver_run_id, rule_id, batch_id)
FOREIGN KEY (silver_run_id) REFERENCES control.silver_run(silver_run_id)
```

**Sample rows**

| silver_run_id | rule_id | batch_id | status | passed_rows | failed_rows |
|---|---|---:|---|---:|---:|
| `silver_20260921_01` | `customer_key_not_null` | `42` | `FAILED` | `998` | `2` |
| `silver_20260921_02` | `country_name_not_null` | `7` | `PASSED` | `195` | `0` |

### Silver DQ violations

**Purpose:** Records a bounded reference to one row-level Silver DQ failure, including a row
diverted by a supported `QUARANTINE` rule. Detailed rows remain in governed Delta evidence.

| Column | Description |
|---|---|
| `silver_run_id`, `rule_id`, `batch_id`, `violation_id` | Required composite retry-stable identity. |
| `record_identity_ref`, `violation_code` | Optional record identity reference and required registered failure reason. |
| `evidence_ref` | Optional reference to detailed governed evidence. |

```text
PRIMARY KEY (silver_run_id, rule_id, batch_id, violation_id)
FOREIGN KEY (silver_run_id, rule_id, batch_id) REFERENCES control.silver_dq_results(silver_run_id, rule_id, batch_id)
```

**Sample rows**

| silver_run_id | rule_id | batch_id | violation_id | violation_code |
|---|---|---:|---|---|
| `silver_20260921_01` | `customer_key_not_null` | `42` | `sv-001` | `NULL_VALUE` |
| `silver_20260921_01` | `customer_key_not_null` | `42` | `sv-002` | `NULL_VALUE` |

### Silver reconciliation violations

**Purpose:** Records one bounded Bronze-input-to-target reconciliation mismatch; it cannot make a
target batch complete when a required reconciliation rule fails.

| Column | Description |
|---|---|
| `silver_run_id`, `rule_id`, `batch_id`, `violation_id` | Required composite identity; `rule_id` resolves to the frozen Silver reconciliation rule. |
| `expected_value`, `actual_value` | Optional canonical bounded comparison values. |
| `evidence_ref` | Optional detailed reconciliation evidence reference. |

```text
PRIMARY KEY (silver_run_id, rule_id, batch_id, violation_id)
FOREIGN KEY (silver_run_id) REFERENCES control.silver_run(silver_run_id)
```

**Sample rows**

| silver_run_id | rule_id | batch_id | violation_id | expected_value | actual_value |
|---|---|---:|---|---|---|
| `silver_20260920_01` | `accepted_matches_mutations` | `41` | `srv-001` | `998` | `997` |
| `silver_20260920_02` | `replace_count_matches_candidate` | `6` | `srv-002` | `195` | `194` |

### Silver reconciliation results

**Purpose:** Records the aggregate outcome of Silver reconciliation processing per run and
micro-batch after target mutation and DQ processing.

| Column | Description |
|---|---|
| `silver_run_id`, `batch_id` | Required composite primary key. |
| `status` | `PENDING`, `PASSED` or `FAILED`. |
| `metrics`, `evidence_ref` | Optional bounded reconciliation metrics and aggregate evidence reference. |

```text
PRIMARY KEY (silver_run_id, batch_id)
FOREIGN KEY (silver_run_id) REFERENCES control.silver_run(silver_run_id)
```

**Sample rows**

| silver_run_id | batch_id | status | metrics | evidence_ref |
|---|---:|---|---|---|
| `silver_20260921_01` | `42` | `PASSED` | `{accepted_rows:998, mutations:998}` | `evidence://silver-recon/42` |
| `silver_20260921_02` | `7` | `PASSED` | `{candidate_rows:195, target_rows:195}` | `evidence://silver-recon/07` |

### Source Cursor

**Purpose:** Persists bounded Source-to-Bronze extraction progress after proven publication; scope:
one bounded producer configuration.

**Description:** It advances only after the related Bronze delivery is complete and is never the
progress store for a Structured Streaming checkpoint.

| Column | Value | Description |
|---|---|---|
| `source_to_bronze_config_id` | 101 | Required PK/FK to a bounded producer configuration whose `checkpoint_ref` is `NULL`. |
| `cursor_schema_ref` | cursor.watermark_pair@1 | Required versioned schema defining how `cursor_payload` is decoded. |
| `cursor_payload` | {"watermark":"2026-09-18T00:00:00Z","tie_breaker":42} | Optional serialized bounded source position; `NULL` before first proven publication or when uninitialized. |
| `source_boundary_ref` | boundary://crm/42 | Optional last proven source boundary; updated only after the related Bronze delivery is complete. |
| `last_delivery_id` | delivery_01 | Optional FK to the publication that justified the current cursor. |
| `last_bronze_delta_version` | 42 | Optional published Bronze Delta version associated with cursor advancement. |
| `row_version` | 7 | Required optimistic-concurrency value for compare-and-swap cursor advancement. |
| `updated_at` | 2026-09-18T10:10:00Z | Required time of last successful cursor advance; never Spark streaming progress. |

```text
PRIMARY KEY (source_to_bronze_config_id)
FOREIGN KEY (source_to_bronze_config_id) REFERENCES metadata.source_to_bronze_config(source_to_bronze_config_id)
FOREIGN KEY (last_delivery_id) REFERENCES control.bronze_manifest(delivery_id)
```

**Sample rows**

| source_to_bronze_config_id | cursor_schema_ref | cursor_payload | last_delivery_id | row_version |
|---:|---|---|---|---:|
| `101` | `cursor.watermark_pair@1` | `{watermark:2026-09-21T00:00Z, tie_breaker:9001}` | `delivery_customer_20260921` | `7` |
| `102` | `cursor.snapshot_token@1` | `NULL` | `delivery_country_20260921` | `1` |
### Lease

**Purpose:** Fences ownership of mutable resources and shared provider capacity; scope: one
canonical resource key.

**Description:** Only the current ACTIVE holder with its fencing token may mutate the resource;
workers stop when renewal or ownership validation fails.

| Column | Value | Description |
|---|---|---|
| `resource_key` | checkpoint:customer_v1 | Required immutable primary key for a query, target relation, checkpoint or provider-capacity resource. |
| `lease_id` | lease_01 | Required current ownership identity; changes when ownership is reacquired after release or expiry. |
| `owner_run_id` | silver_run_01 | Required run or request identity authorized to mutate the resource. |
| `owner_id` | worker_01 | Required executor identity holding the lease. |
| `fencing_token` | 42 | Required monotonic or unique value presented with every protected mutation. |
| `state` | **ACTIVE** | Current owner may mutate the resource with the matching fencing token. |
|  | **RELEASED** | Ownership ended intentionally; no owner may mutate until a new lease is acquired. |
|  | **EXPIRED** | Ownership elapsed without valid renewal; the former worker must stop mutating. |
| `acquired_at` | 2026-09-18T10:00:00Z | Required start time of the current ownership interval. |
| `renewed_at` | 2026-09-18T10:05:00Z | Optional last successful renewal time; `NULL` before first renewal. |
| `expires_at` | 2026-09-18T10:10:00Z | Required expiry time; workers stop when renewal or ownership validation fails. |
| `row_version` | 7 | Required optimistic-concurrency value for acquire, renew and release. |

```text
PRIMARY KEY (resource_key)
```

**Sample rows**

| resource_key | lease_id | owner_run_id | fencing_token | state | expires_at |
|---|---|---|---|---|---|
| `checkpoint:customer_v1` | `lease-501` | `silver_20260921_01` | `501` | `ACTIVE` | `2026-09-21T10:15:00Z` |
| `provider:crm_provider` | `lease-502` | `pipe_20260921_01` | `502` | `RELEASED` | `2026-09-21T10:10:00Z` |
### execution request

**Purpose:** Governs one approved, claimable and terminal one-time operation; scope: one rebuild or
recovery request.

**Description:** The request freezes typed operation parameters and authorization evidence; it is
not durable dataset configuration and an approved request can be claimed only once.

| Column | Value | Description |
|---|---|---|
| `execution_request_id` | request_01 | Required immutable primary key supplied to a governed one-time operation. |
| `operation_type` | REBUILD | Required registered operation handler that selects typed validation and execution behavior. |
| `target_config_ref` | silver.customer:1 | Required logical producer or consumer configuration being operated on. |
| `target_dataset_id` | customer | Required logical dataset identity retained for review and evidence. |
| `contract_version` | 1 | Required positive target contract version. |
| `environment_ref` | prod | Required environment; the request cannot execute in another Workspace environment. |
| `plan_ref` | plan://request_01 | Required frozen operation plan containing approved typed parameters, source boundary and selection. |
| `artifact_ref` | framework@0.1.0 | Required approved framework or domain artifact identity. |
| `configuration_hash` | sha256:... | Required selected metadata and configuration hash; detects changes after approval. |
| `binding_hash` | sha256:... | Required resolved environment-binding hash; detects topology or connection changes after approval. |
| `requested_by` | operator@example.com | Required identity of the request creator. |
| `requested_at` | 2026-09-18T10:00:00Z | Required request creation time that starts its lifecycle. |
| `approved_by` | approver@example.com | Optional approver identity; required before `APPROVED` can transition to `CLAIMED`. |
| `approved_at` | 2026-09-18T10:05:00Z | Optional approval time; `NULL` until a decision is recorded. |
| `reason_ref` | INC-12345 | Required bounded reason or ticket reference. |
| `expires_at` | 2026-09-19T10:00:00Z | Required request or approval expiry time; expired requests cannot be claimed. |
| `status` | **PLANNED** | Typed plan is frozen but not yet approved. |
|  | **APPROVED** | Governed approval is recorded and the request is eligible for a single claim. |
|  | **CLAIMED** | One production worker or Pipeline has atomically claimed the request. |
|  | **RUNNING** | The claimed operation is executing. |
|  | **SUCCEEDED** | The operation completed successfully with terminal evidence. |
|  | **FAILED** | The operation ended unsuccessfully with terminal evidence. |
|  | **CANCELLED** | The operation was intentionally stopped. |
|  | **REJECTED** | Approval was denied. |
|  | **EXPIRED** | The request or approval expired before a valid claim completed. |
| `claimed_by` | fabric_pipeline_01 | Optional worker or Pipeline identity set atomically on claim. |
| `claimed_at` | 2026-09-18T10:06:00Z | Optional claim time; `NULL` before `CLAIMED`. |
| `completed_at` | 2026-09-18T10:20:00Z | Required for terminal statuses. |
| `outcome_ref` | evidence://request_01 | Optional bounded reference to resulting run or evidence records. |

```text
PRIMARY KEY (execution_request_id)
```

**Sample rows**

| execution_request_id | operation_type | target_config_ref | status | requested_by | reason_ref |
|---|---|---|---|---|---|
| `request-301` | `REBUILD` | `silver.customer:1` | `APPROVED` | `operator@example.com` | `INC-12345` |
| `request-302` | `RECOVER` | `source.crm_customer:1` | `SUCCEEDED` | `operator@example.com` | `INC-12346` |
### audit event

**Purpose:** Retains immutable configuration, governance, lifecycle and outcome transitions; scope:
one recorded event and its related entities.

**Description:** It correlates actors and related configuration, run or request identities while
referencing detailed evidence instead of duplicating unbounded diagnostics or business metrics.

| Column | Value | Description |
|---|---|---|
| `event_id` | event_01 | Required immutable primary key and physical correlation key for idempotent event recording. |
| `event_type` | CONFIGURATION_CHANGED | Required registered event type; identifies a configuration, planning, lease, callback, approval or outcome transition. |
| `actor_id` | worker_01 | Required actor or service identity that caused or reported the event. |
| `occurred_at` | 2026-09-18T10:10:00Z | Required event time supplied by the recording boundary. |
| `config_ref` | source.crm_customer:1 | Optional producer or consumer configuration for a metadata-scoped event. |
| `run_id` | run_01 | Optional generic run identity when a specific run column is not used. |
| `pipeline_run_id` | run_01 | Optional FK to the parent pipeline invocation. |
| `bronze_run_id` | bronze_run_01 | Optional FK to the associated producer execution. |
| `silver_run_id` | silver_run_01 | Optional FK to the associated consumer execution. |
| `execution_request_id` | request_01 | Optional FK to the associated governed request. |
| `attempt_id` | attempt_01 | Optional stable callback-attempt identity shared by its started and terminal events. |
| `query_identity` | customer_v1_generation_1 | Optional Silver checkpoint-generation identity; required for batch or callback events. |
| `batch_id` | 42 | Optional Spark micro-batch ID; required with `query_identity` for batch or callback events. |
| `owner_id` | worker_01 | Optional lease or fencing owner that held execution ownership. |
| `fencing_token` | 42 | Optional ownership proof used by a protected transition. |
| `target_commit_ref` | delta://silver/customer#42 | Optional target Delta commit evidence; identifies a mutation without duplicating metrics. |
| `error_code` | TARGET_WRITE_FAILED | Optional bounded error category; detailed stack traces remain in execution evidence. |
| `evidence_ref` | evidence://event_01 | Optional bounded diagnostics, reconciliation or outcome evidence reference. |

```text
PRIMARY KEY (event_id)
FOREIGN KEY (pipeline_run_id) REFERENCES control.pipeline_run(pipeline_run_id)
FOREIGN KEY (bronze_run_id) REFERENCES control.bronze_run(bronze_run_id)
FOREIGN KEY (silver_run_id) REFERENCES control.silver_run(silver_run_id)
FOREIGN KEY (execution_request_id) REFERENCES control.execution_request(execution_request_id)
```

**Sample rows**

| event_id | event_type | actor_id | bronze_run_id | silver_run_id | evidence_ref |
|---|---|---|---|---|---|
| `event-1001` | `BRONZE_PUBLICATION_COMPLETED` | `worker-01` | `bronze_20260921_01` | `NULL` | `evidence://delivery_customer_20260921` |
| `event-1002` | `SILVER_BATCH_COMPLETED` | `worker-02` | `NULL` | `silver_20260921_01` | `evidence://silver-recon/42` |
## Public procedure interfaces

The default SQL adapter exposes versioned public procedures, not private table writes. Runtime state
is written through the framework control-plane port. The names below describe planned default-SQL
interface families; they are illustrative, not implemented SQL APIs or finalized signatures. A
company/domain adapter exposes equivalent semantic operations through its own transport. Public
inspection views read these tables without mutating them. No procedure implements business-data
algorithms.

| Planned interface / example name | Tables read or changed | Purpose |
|---|---|---|
| metadata.usp_upsert_execution_group | metadata.execution_group | Register the group referenced by producer/consumer configs. |
| metadata.usp_upsert_source_to_bronze_config | Producer config | Idempotently resolve/create its logical dataset/version and attach routing. |
| metadata.usp_upsert_bronze_to_silver_config | Consumer config; reads producer config | Resolve the producer FK and register independent target/checkpoint settings. |
| metadata.usp_upsert_bronze_policy | `metadata.bronze_policy`; reads source config | Resolve the owner and validate common plus extraction/representation settings. |
| metadata.usp_upsert_silver_policy | `metadata.silver_policy`; reads consumer config | Resolve the owner and validate common plus load-strategy settings. |
| metadata.usp_upsert_bronze_dq_rule | `metadata.bronze_dq_rule`; reads source config/policy | Upsert a registered producer DQ check by owner plus stable rule ID. |
| metadata.usp_upsert_bronze_recon_rule | `metadata.bronze_recon_rule`; reads source config/policy | Upsert a registered producer reconciliation check by owner plus stable rule ID. |
| metadata.usp_upsert_silver_dq_rule | `metadata.silver_dq_rule`; reads consumer config/policy | Upsert an independently owned Silver DQ check by owner plus stable rule ID. |
| metadata.usp_upsert_silver_recon_rule | `metadata.silver_recon_rule`; reads consumer config/policy | Upsert an independently owned Silver reconciliation check by owner plus stable rule ID. |
| metadata.usp_set_config_enabled | Selected producer/consumer config | Pause/resume future normal planning per configuration; audit changes without cancelling active runs or deleting resources/checkpoints. |
| control.usp_plan_execution_group | Enabled metadata; creates Pipeline, Bronze and Silver runs | Validate enabled group members, dependency graph, bindings and provider capacity; freeze bounded job plans. |
| control.usp_get_execution_group_bronze_deliveries | Frozen Bronze runs and rule bindings | Return every `bronze_run_id` selected by one parent pipeline run with its source object, ingestion-method reference, and ordered DQ/reconciliation rule references. |
| control.usp_get_bronze_reader_plan | Frozen Bronze run plan | Return the source object, capture mode, optional watermark column, frozen relation and boundary reference for one opaque Bronze run. |
| control.usp_get_silver_consumer_plan | Frozen Silver run plan | Return the retained Bronze input, Silver target, checkpoint, query identity, load strategy, and typed Silver policy for one opaque Silver run. |
| control.usp_record_bronze_manifest | `control.bronze_manifest`; reads Bronze run | Publish consistent bounded delivery/completeness evidence. |
| Bronze/Silver DQ result procedures | `control.bronze_dq_results`, `control.silver_dq_results` | Idempotently record aggregate DQ outcomes for the stage, rule and optional delivery/batch. |
| Bronze/Silver DQ violation procedures | `control.bronze_dq_violation`, `control.silver_dq_violation` | Idempotently record bounded row-level violation references. |
| Bronze/Silver reconciliation violation procedures | `control.bronze_recon_violation`, `control.silver_recon_violation` | Idempotently record bounded expected-versus-actual mismatches. |
| Bronze/Silver reconciliation-result procedures | `control.bronze_recon_results`, `control.silver_recon_results` | Idempotently record aggregate reconciliation outcome and evidence. |
| control.usp_advance_source_cursor | `control.source_cursor`; reads publication/lease evidence | Advance a bounded producer's cursor with compare-and-swap after proven publication. |
| Lease acquire/renew/release procedures | `control.lease`; reads owner run/resource scope | Claim/fence queries, targets, checkpoints and provider slots; never modify Spark checkpoint files. |
| control.usp_record_silver_manifest | `control.silver_manifest`, attempt audit events; reads Silver run/query binding | Idempotently record batch effects, reconciliation and completion while preserving attempt history. |
| Run outcome recording procedures | Bronze/Silver runs; reads manifests | Record execution status and expose summaries derived from completed batch evidence. |
| Execution-request planning procedure | `control.execution_request`; reads config/bindings | Validate a registered typed handler and parameters, then freeze a one-time plan as PLANNED; no execution authorization yet. |
| Request approval/rejection procedure | `control.execution_request`, `control.audit_event` | Record the governed approval decision; protected environments require approval before claim. |
| Request claim procedure | Request, lease and production run state | Atomically claim one eligible, unexpired approved request for one production execution. |
| Request completion/cancellation procedure | Request, run and audit state | Record fenced lifecycle transitions and terminal evidence; a successful request is not reusable. |

For example, `control.usp_plan_execution_group(execution_group_id, environment, fabric_pipeline_run_id)`
returns opaque job-run identities. The Pipeline does not join private tables or interpret policies.
See [Annotated SQL](../examples/CONTROL_PLANE_CONFIGURATION.md#annotated-desired-state-sql) for
configuration calls and prerequisite policy setup.

### Framework SQL reader manager

The framework-provided `BronzeControlPlaneManager` uses
`control.usp_get_bronze_reader_plan` to supply the reader-specific projection of a planned Bronze
run. The procedure returns exactly one row with `bronze_run_id`, `source_to_bronze_config_id`,
`bronze_relation_ref`, `source_boundary_ref`, `source_object_ref`, `capture_mode`, and
`watermark_column`. It MUST read the run's frozen plan, not join mutable desired-state metadata at
execution time.

A source capability proves the concrete snapshot version or watermark interval, then the manager
combines that boundary with the procedure result to create the reader request. The reader returns
source facts and bounded `SourceReadEvidence`; it does not write business rows to Fabric SQL. After
Bronze publication, the manager passes the evidence's optional selected-row count with publication
facts to `control.usp_record_bronze_manifest`. A reader result alone MUST NOT advance
`control.source_cursor`; only the fenced cursor procedure may do so after complete publication.

`SilverControlPlaneManager` uses `control.usp_get_silver_consumer_plan`, which returns exactly one
frozen projection with `silver_run_id`, both configuration IDs, `bronze_relation_ref`,
`silver_relation_ref`, `checkpoint_ref`, `query_identity`, `load_strategy`, and every typed
`metadata.silver_policy` field. Array and schema fields are returned as JSON arrays; typed delete
marker values retain their JSON types. The procedure MUST return the run's frozen plan rather than
mutable desired-state metadata. A consumer applies that plan in Spark and sends only its bounded
micro-batch counts, target commit reference, reconciliation reference, and completion state to
`control.usp_record_silver_manifest`.

Both managers extend the abstract `ControlPlaneManager` extension point. A deployment can subclass
either manager to customize only the relevant plan-loading or evidence-recording behavior without
coupling Bronze ingestion to Silver consumption.

`ExecutionGroupControlPlaneManager` is the producer scheduling adapter for a parent
`pipeline_run_id`. It reads only `control.usp_get_execution_group_bronze_deliveries`, which returns
already frozen table deliveries rather than mutable metadata. It records each table's DQ-rule
results, DQ violation references, reconciliation-rule results, aggregate reconciliation outcome,
and terminal Bronze-run outcome through separate bounded audit procedures. A runtime factory
constructs the selected ingestion/DQ/reconciliation capabilities for each returned table delivery;
the control-plane manager persists evidence but does not execute business-data algorithms.

All physical transports inherit the minimal `Connector` base. It intentionally does not define a
universal `connect` or `read` operation because Spark-native source connectors and database
connectors have different connection lifecycles. Every concrete connector MUST implement
`health()`, which returns bounded `HEALTHY`, `UNHEALTHY`, or `NOT_CONFIGURED` evidence without
exposing secrets. Connectors MAY release resources through `close()`.

Source connectors retain their separate structural `read()` capability. They use an optional
configured health-check resource and force a bounded Spark action when it is present; otherwise
they report `NOT_CONFIGURED`. A source credential rotation resolves a fresh profile or secret and
creates a replacement connector; it does not pretend that Spark can renew the old connector in
place.

`FabricSqlDatabase` adds `new_session()` and `renew()` beyond the base contract. It tests a fresh
session with `SELECT 1` and renews by dropping its pooled connections. `ControlPlaneManager` owns
parameterized SQL reads and transactional commands over those sessions. An on-premises SQL Server
adapter can inherit `Connector`, expose compatible `new_session()`, `connect()`, and `begin()`
operations, and be supplied to either manager without changing Spark reader or consumer code.

`ApiConnector` adds `new_session()` and `renew()` beyond the base contract. A session snapshots the
current bearer token; `renew()` replaces that token only through a deployment-supplied provider, so
tokens never appear in control-plane metadata or health output. Existing API sessions retain their
token snapshot and new sessions use the renewed token.
