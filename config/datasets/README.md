# Dataset configuration

This directory will contain logical dataset definitions: capture strategy, Silver load strategy,
keys, ordering, schema, quality policy, reconciliation policy and dependencies.

Where applicable, a definition must explicitly declare:

- Bronze representation: `SNAPSHOT`, `EVENT_LOG`, `CURRENT_STAGE` or governed `EPHEMERAL`;
- entity key, event identity and source cursor as separate roles;
- CDC delete semantics;
- SCD2 late-arrival policy and bounded window;
- schema policy: `STRICT` or explicitly enabled `ADDITIVE_NULLABLE`; and
- logical source, Bronze and Silver `RelationRef` values.

Missing or incompatible semantics fail validation/plan compilation. There is no implicit delete
policy, event-hash fallback, Bronze mode or globally enabled schema merge.

`run_mode` is not a dataset setting. One-time rebuild/backfill/recovery intent is stored in the
control plane and referenced at execution by an opaque request ID; it is never committed to Git.

A dataset definition identifies relations through logical references. Physical workspace,
Lakehouse, schema, table and connection values are supplied by `config/environments/`.
