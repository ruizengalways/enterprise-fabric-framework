# Enterprise Fabric Framework

A clean-start, Spark-first data framework for Microsoft Fabric.

This repository is currently an architecture scaffold. It intentionally contains no
in-memory implementation of APPEND, REPLACE, UPSERT, SCD1, SCD2, SNAPSHOT_DIFF, CDC,
or current projection semantics.

Its product boundary is `enterprise source -> Bronze -> Silver`. Gold models, marts, KPIs,
Power BI semantic models and consumer-serving releases belong to independently owned domain
repositories.

## Architectural baseline

- Spark and Delta are the only production execution model for business datasets.
- Python owns configuration, orchestration, control-plane state, audit and bounded evidence.
- Production data remains in Spark DataFrames, Spark SQL or Delta relations.
- Production paths must not collect a table to the driver or call `toPandas()`.
- Dev, UAT and Prod run the same package and code path; only environment bindings change.
- CI executes the same Spark runtime entry point used by Fabric jobs.
- Certification invokes production runtime code; it does not contain a second business engine.
- Silver is the final data-product boundary of this framework; it does not implement a shared Gold
  layer.
- Pure-Python in-memory business algorithms are not implemented, maintained or used as test
  oracles; they would create a second system whose success does not prove Fabric behavior.

Start with:

- [Spark-first architecture](docs/architecture/SPARK_FIRST_ARCHITECTURE.md)
- [Enterprise data patterns](docs/architecture/DATA_PATTERNS.md)
- [Silver contract versioning](docs/architecture/SILVER_VERSIONING.md)
- [Package boundaries](docs/architecture/BOUNDARIES.md)
- [Testing strategy](docs/architecture/TESTING_STRATEGY.md)
- [Dataset rebuild runbook](docs/operations/REBUILD_RUNBOOK.md)
- [Architecture decision records](docs/adr/README.md)

## Repository map

```text
config/                         Logical datasets and environment bindings
docs/                           Architecture and decision records
src/enterprise_fabric_framework/
  config/                       Config loading and environment resolution
  metadata/                     Dataset semantics and declared capabilities
  contracts/                    Stable cross-layer value contracts
  utils/                        Small deterministic, side-effect-free utilities
  spark/                        The only production business-data runtime
  control_plane/                Small SQLAlchemy-backed operational state
  orchestration/                Planning, dependency scheduling and run coordination
  platform/fabric/              Fabric REST, identity, item and job adapters
  recovery/                     Governed checkpoint and target recovery
  certification/                Thin production-path certification runners
  deployment/                   Packaging and deployment materialization
  extensions/                   Governed Spark-native extension points
  cli/                          Operator entry points
tests/
  unit/                         Python-only tests
  spark/                        Local Spark and Delta tests
  fabric/                       Real Fabric integration tests
  uat/                          End-to-end UAT scenarios
docker/spark-test/              Pinned local Spark/Delta test image, added after runtime selection
.github/workflows/              CI, nightly, Fabric UAT and production deployment automation
```

The initial runtime baseline is Fabric Runtime 2.0: Spark 4.1, Delta 4.2, Python 3.13 and Java 21.

No production implementation should be added until the proposed ADRs have been reviewed.
