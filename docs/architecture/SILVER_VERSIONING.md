# Silver contract versioning and parallel migration

Status: accepted architecture under ADR 0018 and ADR 0020.

## Two different operations

Routine data replacement and a breaking Silver redesign are not the same operation.

### Routine REPLACE

Use the existing Silver contract and stable physical Delta relation when schema, grain, keys and
semantics remain compatible. Under ADR 0020, every production REPLACE builds a run-scoped physical
candidate, validates it in Spark, publishes it to the stable Silver relation and records
commit/reconciliation evidence. It does not create `v2` merely because the data was refreshed.

### Breaking contract migration

Create a parallel Silver contract version when any of these change incompatibly:

- business grain or keys;
- current/history or delete semantics;
- canonical schema or type meaning;
- source-history fidelity;
- transformation meaning in a way that would change existing consumers; or
- a correction that must be validated while the old result remains available.

The logical identities are independently addressable, for example `customer_silver:v1` and
`customer_silver:v2`. Physical suffixes such as `customer_silver_v2` are environment bindings, not
names embedded in strategy code.

## Scenario A: Bronze is valid and only Silver changes

```text
                         +-> Silver v1 -> existing domain consumers
shared valid Bronze -----+
                         +-> Silver v2 -> validation -> migrated domain consumers
```

1. Freeze a source/Bronze boundary for the v2 bootstrap.
2. Build Silver v2 from durable Bronze evidence up to that boundary.
3. Give v2 an independent target state and checkpoint/projection progress.
4. Fan subsequent Bronze batches to both Silver versions while migration is active.
5. Validate v2 with Spark invariants, reconciliation, known business cases and domain acceptance;
   v1 is not assumed to be the correct oracle.
6. Each domain repository changes its own environment binding from Silver v1 to v2 and deploys its
   Gold changes independently.
7. Stop v1 only after registered consumers have migrated or explicitly accepted retirement.

If Bronze is `CURRENT_STAGE` or `EPHEMERAL` and cannot replay the required history, v2 needs a new
authoritative baseline. The framework must not claim it reconstructed unavailable history.

## Scenario B: Bronze and Silver are both wrong

```text
source -> Bronze v1 -> Silver v1 -> unchanged existing consumers
      \-> Bronze v2 -> Silver v2 -> validation -> migrated domain consumers
```

1. Leave the v1 chain unchanged while it still serves consumers.
2. Create independently bound Bronze v2 and Silver v2 relations and control-plane identities.
3. Re-extract an authoritative baseline for Bronze v2 when the source supports it. Otherwise retain
   and declare the exact fidelity limitation of the recoverable evidence.
4. Capture a no-gap handoff boundary, then catch v2 up with incrementals.
5. Run v1 and v2 in parallel only for the bounded migration period.
6. Domain repositories opt into Silver v2 through their own reviewed binding change.
7. Pause v1 ingestion/processing after cutover, retain it for the approved rollback window, then
   decommission it through a separate governed cleanup workflow.

## Cutover ownership

This framework publishes and evidences Bronze/Silver contract versions. It does not repoint Gold,
Power BI or reporting objects. Each domain repository owns its consumer change.

The framework may expose bounded readiness such as `BUILDING`, `READY_FOR_CONSUMER_VALIDATION`,
`ACTIVE`, `DEPRECATED` and `RETIRED`. It must not mark v2 active merely because its Spark job passed;
domain acceptance and the organization's consumer inventory remain external governance inputs.

## Retirement rules

- Do not overwrite v1 with v2 while any v1 consumer remains. A version label is a contract, not a
  temporary table name.
- Prefer retiring and eventually deleting v1 rather than replacing its contents with v2 semantics.
- Keep rollback long enough to cover consumer validation, checkpoint catch-up and required audit
  retention.
- Stop compute separately from deleting data. A paused v1 can remain readable during the grace
  period.
- DROP/VACUUM is a separately authorized, idempotent cleanup operation with dry-run inventory and
  retained evidence.
- A shallow clone is suitable only for short-lived acceleration. It depends on source data files,
  so source VACUUM is prohibited until clone dependencies are removed or materialized independently.

## Recommended framework contract

Model version migration explicitly rather than embedding it in ordinary REPLACE:

- `dataset_id`: stable logical dataset family;
- `contract_version`: immutable semantic version identity;
- `RelationRef`: environment-resolved physical Bronze/Silver target;
- independent target/checkpoint state per contract version;
- declared upstream Bronze contract version;
- lifecycle status and replacement relationship;
- bounded build/reconciliation evidence; and
- optional consumer-governance reference without Gold implementation details.

Routine REPLACE and breaking migration are now separate accepted mechanisms: ADR 0020 governs
validated candidate publication under an unchanged contract, while ADR 0018 governs parallel
contract versions.

Individual bootstrap, rebuild, catch-up and retirement actions are submitted as governed one-time
control-plane requests under ADR 0019. Version-controlled dataset definitions describe v1/v2
contracts but do not contain mutable operation flags.
