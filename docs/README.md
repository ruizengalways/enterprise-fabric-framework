---
id: documentation.index
status: current
last_reviewed: 2026-09-17
---

# Documentation index

These documents describe the current framework. Each topic has one source of truth; other files
link to it rather than restating its rules.

## Canonical documents

| Document | Source of truth for |
|---|---|
| [Expected usage model](USAGE_MODEL.md) | intended users, domain workflows, Pipeline usage and dataset onboarding |
| [System architecture](architecture/SYSTEM.md) | product boundary, invariants, runtime flow, package ownership and glossary |
| [Data lifecycle](architecture/DATA_LIFECYCLE.md) | source facts, bounded/streaming ingress, append-only Bronze, streaming Silver and versioning |
| [Control plane](architecture/CONTROL_PLANE.md) | table catalog, policy/rule fields and relationships, public procedures, runtime state and SQL delivery lifecycle |
| [Execution evidence](architecture/CONTROL_PLANE_EVIDENCE.md) | Silver manifest fields, commit recovery, attempt history, run summaries and bounded evidence |
| [Control-plane examples](examples/CONTROL_PLANE_CONFIGURATION.md) | configuration values and annotated illustrative SQL; contracts remain in architecture |
| [Fabric delivery](architecture/FABRIC_DELIVERY.md) | workspaces, Pipelines, ingress handoff, environment binding and CI/CD |
| [Testing](architecture/TESTING.md) | unit, SQL, local Spark/Delta, Fabric UAT and release gates |
| [Rebuild runbook](operations/REBUILD.md) | operator procedure for a governed dataset rebuild |

Create `decisions/OPEN.md` only when an unresolved contract decision requires owner input; delete it
when none remain. There are currently no open decisions.

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
AGENTS.md -> this index -> USAGE_MODEL -> relevant canonical document -> OPEN (if present)
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
