---
id: architecture.source-to-bronze-lifecycle
status: current
source_of_truth_for:
  - source-facts
  - bounded-and-streaming-ingress
  - capture-semantics
  - bronze-representations
  - bronze-schema-evolution
last_reviewed: 2026-09-22
---

# Source-to-Bronze lifecycle

## Capture semantics

Every source-to-Bronze producer appends the source facts it reads. This table contains only source
columns and static reader configuration required to implement the capture. Run boundaries,
completeness evidence, delivery identifiers, and other derived execution facts are not capture
parameters.

| Capture category | Capture parameters | Description |
|---|---|---|
| **Complete snapshot** | `source_object` - mandatory | Table or query to read, for example `crm.dbo.customer`. |
| **Watermark with lookback** | `watermark_column` - mandatory | Changed-row field, for example `modified_at`. |
|  | `watermark_lookback_seconds` - mandatory | Re-read overlap, for example `7200` seconds. |
| **Watermark with lookback + delete flag** | `watermark_column` - mandatory | Changed-row field, for example `modified_at`. |
|  | `watermark_lookback_seconds` - mandatory | Re-read overlap, for example `7200` seconds. |
|  | `delete_indicator_column` - mandatory | Retained source marker that identifies deleted rows, for example `is_deleted`. |
|  | `delete_indicator_values` - mandatory | Values or encoding that mean deleted, for example `true`. |
| **Net-change CDC** | `entity_identity_columns` - conditional | Required when no change identity is supplied, for example `customer_id`. |
|  | `source_position_columns` - mandatory | Ordered source position and tie breaker, for example `commit_lsn`, `change_id`. |
|  | `source_operation_column` - mandatory | Operation column and encodings, for example `operation` with `I`, `U`, and `D`. |
| **All-change CDC** | `change_identity_column` - mandatory | Stable identity of each change, for example `change_id`. |
|  | `source_position_columns` - mandatory | Ordered source position and tie breaker, for example `commit_lsn`, `sequence`. |
|  | `source_operation_column` - mandatory | Operation column and encodings, for example `operation` with `I`, `U`, and `D`. |
|  | `entity_identity_columns` - mandatory | Identity of the changed entity, for example `customer_id`. |
| **Business events/audit** | `event_identity_column` - mandatory | Stable identity of each immutable event, for example `event_id`. |
|  | `event_ordering_columns` - mandatory | Event order and tie breaker, for example `occurred_at`, `event_id`. |
|  | `event_timestamp_column` - optional | Source event time when supplied, for example `occurred_at`. |
| **File drop** | `source_location` - mandatory | Landing location, for example `abfss://landing@workspace.dfs.fabric.microsoft.com/customer`. |
|  | `file_format` - mandatory | File format, for example `parquet`. |
|  | `file_selection_pattern` - mandatory | Pattern selecting files, for example `customer_*.parquet`. |
|  | `record_identity_columns` - optional | Source-row identity when available, for example `customer_id`. |
|  | `ordering_columns` - optional | Ordering fields when available, for example `file_date`, `row_number`. |

For CDC, every supplied change, including a delete operation or tombstone, is appended as a retained
source fact. For business events, every immutable event is appended with its stable event identity.
Append-only Bronze therefore works for both patterns when the required identity and ordering fields
permit replay and deterministic capture. It does not restore intermediate changes that a net-change
provider has already collapsed.


