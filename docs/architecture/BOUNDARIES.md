# Package boundaries

Status: proposed baseline for owner review.

## Product boundary

This package owns source capture, Bronze and Silver. It ends after a governed Silver Delta commit,
reconciliation and checkpoint/audit completion.

Independent domain repositories own Gold models, marts, aggregates, KPIs, Power BI semantic
models, reports and consumer-facing publication. The framework may expose a stable Silver contract
to those repositories, but it does not deploy or coordinate their Gold implementations.

## Dependency direction

```text
config/metadata ---> contracts <--- control_plane
       |                ^               ^
       v                |               |
orchestration ------> spark/runtime ----+
       |                |
       v                v
platform/fabric     Spark/Delta relations

certification ---> public orchestration/runtime entry only
```

## Ownership

| Package | Owns | Must not own |
|---|---|---|
| `config` | loading, overlays, environment binding | business-data transforms |
| `metadata` | dataset semantics and capability declarations | executor implementations |
| `contracts` | immutable cross-layer values | Spark actions or SQL connections |
| `utils` | small deterministic primitives with no I/O | domain workflows or Spark row algorithms |
| `spark` | source-to-Bronze and Bronze-to-Silver capture, transform, load, reconciliation and projection | Gold/domain models, Fabric REST or durable control state |
| `control_plane` | leases, checkpoints, audits, one-time execution requests and attempts | business table contents or Git configuration |
| `orchestration` | planning, request claim, dependency scheduling and runtime coordination | strategy algorithms or approval policy storage |
| `platform/fabric` | identity, REST, items, jobs and physical bindings | business semantics |
| `recovery` | governed Bronze/Silver recovery decisions and replacement coordination | Gold cutover or a second implementation of load semantics |
| `certification` | scenarios, invocation and retained evidence | APPEND/SCD algorithms |
| `extensions` | registered Spark-native extension contracts | arbitrary row-list apply callbacks |
| `cli` | operator-facing plan/show/execute/cancel commands over the same control-plane API | a second execution path or direct operational-table edits |

## Utility boundary

`utils/temporal.py` owns driver-side UTC primitives. `utils/hashing.py` owns deterministic hashes
for small configuration, contract and evidence objects.

Business-row hashing is different: it must use Spark expressions over a declared schema, with
explicit null, timestamp, decimal, binary and nested-value canonicalization. It therefore belongs
in `spark/transform/hashing.py`, not in generic Python utilities. This prevents Python/Spark hash
drift and avoids collecting rows to the driver.

The `utils` directory is not a general dumping ground. A module belongs there only if it is:

- domain-neutral;
- deterministic and side-effect-free;
- independent of Spark sessions, Fabric clients and SQL connections;
- safe for small control/configuration values.
