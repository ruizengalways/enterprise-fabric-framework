# ADR 0001: Spark and Delta are the only production business-data runtime

- Status: Proposed
- Date: 2026-09-15

## Context

Maintaining complete Python row-list and Spark implementations creates two semantic authorities.
It also encourages tests to validate a non-production engine before comparing it with Spark.
The company production environment executes these workloads in Microsoft Fabric, so a pure-Python
business-data engine has no production consumer. Passing its tests cannot prove that Spark SQL,
Delta transactions, OneLake, Fabric catalogs or the managed Fabric runtime behave correctly.

## Decision

All production capture normalization, business transformation, data quality, load,
reconciliation scans and projections operate on Spark DataFrames, Spark SQL or Delta relations.
Python remains responsible for configuration, orchestration, control-plane state and bounded
evidence. No production fallback to an in-memory business algorithm is permitted.

The framework will not implement, port or maintain pure-Python in-memory versions of APPEND,
REPLACE, UPSERT, SCD1, SCD2, SNAPSHOT_DIFF, CDC processing or current projection. This prohibition
also applies to reference implementations, certification probes, fallback executors and test
oracles. Business-semantic tests must execute the production Spark code path.

Python-only tests remain appropriate for bounded concerns that do not process business tables:
configuration, immutable contracts, orchestration decisions, control-plane state, audit,
environment binding and platform protocol handling.

## Consequences

- CI requires real local Spark/Delta execution.
- Small Python fixtures may construct Spark DataFrames but are not an alternative runtime or
  semantic oracle.
- Reusable semantics are expressed as contracts, Spark expressions and invariant tests.
- Control-plane SQLAlchemy usage remains valid because it handles bounded operational state.
- There are no Python-versus-Spark parity tests because there is no second business engine.
- Local Spark/Delta success is necessary but does not certify Microsoft Fabric behavior. Release
  capability requires the same candidate wheel and runtime path to pass real Fabric UAT.
