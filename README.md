# Enterprise Fabric Framework

A Spark-first Microsoft Fabric framework for:

```text
enterprise source -> Bronze -> Silver
```

This repository is currently an architecture scaffold. Production business-data semantics will
run only in Spark/Delta; the project does not maintain an in-memory Python implementation or test
oracle. Gold models and consumer-serving releases belong to independently owned domain repositories.

## Start here

- [Documentation index](docs/README.md)
- [Expected usage model](docs/USAGE_MODEL.md)
- [System architecture](docs/architecture/SYSTEM.md)

Coding agents must also follow [AGENTS.md](AGENTS.md).

## Repository areas

```text
docs/                       canonical architecture, decisions and runbooks
src/enterprise_fabric_framework/
  contracts/                immutable cross-layer contracts
  metadata/                 typed policy mapping and capability selection
  control_plane/            replaceable control-plane port and default SQL adapter
  orchestration/            planning and runtime coordination
  platform/fabric/          invocation and binding adapters
  spark/                    the only business-data runtime
  recovery/                 governed recovery coordination
  certification/            production-path certification runners
  cli/                      operator entry points
  utils/                    bounded deterministic utilities
sql/control_plane/          reusable SQL schema and migration artifacts
tests/                      unit, SQL, local Spark and real Fabric suites
  fabric/items/             native Fabric items for framework integration/UAT tests
```

Detailed ownership and the target package tree are canonical in
[SYSTEM.md](docs/architecture/SYSTEM.md). Test layout is canonical in
[TESTING.md](docs/architecture/TESTING.md).

## Repository boundary

This is the reusable framework repository. A production domain has its own repository, isolated
Dev/UAT/Prod workspaces and a control-plane implementation. The framework supplies a default SQL
adapter, while a company or domain may provide another adapter that satisfies the same typed port.
Domain repositories own Fabric items, idempotent metadata desired state and Gold logic; they select
versioned framework capabilities rather than supplying private source-to-Silver Python plugins.
