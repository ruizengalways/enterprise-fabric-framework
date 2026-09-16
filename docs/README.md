---
id: documentation.index
status: current
last_reviewed: 2026-09-16
---

# Documentation index

These documents describe the current framework. Each topic has one source of truth; other files
link to it rather than restating its rules.

## Canonical documents

| Document | Source of truth for |
|---|---|
| [Expected usage model](USAGE_MODEL.md) | intended users, domain workflows, Pipeline usage and dataset onboarding |
| [System architecture](architecture/SYSTEM.md) | product boundary, invariants, runtime flow, package ownership and glossary |
| [Data lifecycle](architecture/DATA_LIFECYCLE.md) | source facts, capture, Bronze representations, Silver strategies and versioning |
| [Control plane](architecture/CONTROL_PLANE.md) | SQL metadata, typed policies, frozen runs, registry, evidence, checkpoints and requests |
| [Fabric delivery](architecture/FABRIC_DELIVERY.md) | workspaces, Pipelines, ingress handoff, environment binding and CI/CD |
| [Testing](architecture/TESTING.md) | unit, SQL, local Spark/Delta, Fabric UAT and release gates |
| [Open decisions](decisions/OPEN.md) | unresolved choices requiring owner input |
| [Rebuild runbook](operations/REBUILD.md) | operator procedure for a governed dataset rebuild |

`decisions/OPEN.md` is deleted when no unresolved contract decision remains.

## Reading paths

New framework developer:

```text
USAGE_MODEL -> SYSTEM -> DATA_LIFECYCLE -> CONTROL_PLANE -> TESTING
```

Domain data engineer:

```text
USAGE_MODEL -> DATA_LIFECYCLE -> FABRIC_DELIVERY -> TESTING
```

Platform or release engineer:

```text
USAGE_MODEL -> SYSTEM -> FABRIC_DELIVERY -> CONTROL_PLANE -> TESTING
```

Coding agent:

```text
AGENTS.md -> this index -> USAGE_MODEL -> relevant canonical document -> OPEN
```

## Documentation rules

- Current behavior belongs in `architecture/`, not in the open-decisions file.
- A normative rule has one canonical owner.
- Use `MUST`, `SHOULD` and `MAY` for mandatory, recommended and optional behavior.
- Keep stable headings so code, tests and discussions can link to them.
- Record external version facts with a verification date and primary-source link.
- Unresolved owner-response placeholders may appear only in `decisions/OPEN.md`.
- Operational steps belong in `operations/`; they MUST call the same production path.
- A canonical document SHOULD stay below 500 lines. Split by responsibility, not by arbitrary size.
