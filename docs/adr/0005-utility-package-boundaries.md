# ADR 0005: Utility package boundaries

- Status: Proposed
- Date: 2026-09-15

## Decision

Widely reused, deterministic and side-effect-free primitives live under `utils/`. UTC handling
belongs in `utils/temporal.py`. Canonical hashing for small configuration/evidence objects belongs
in `utils/hashing.py`.

Distributed business-row hashing remains under `spark/transform/hashing.py` because it must be a
Spark Column expression with schema-aware canonicalization. Generic utilities must not import
Spark, Fabric clients, SQLAlchemy repositories or domain strategy modules.

## Consequences

- Common utilities are easy to find without becoming a dependency dumping ground.
- Driver-side hashes and Spark row hashes have intentionally separate APIs and test suites.
- Row hashing never requires collecting business rows into Python.
