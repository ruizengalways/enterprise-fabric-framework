# ADR 0002: One runtime registry is the capability source of truth

- Status: Proposed
- Date: 2026-09-15

## Context

Independent capability declarations, plan compilation rules and executor resolvers can drift.
A plan can then advertise a strategy that cannot be executed.

## Decision

The Spark runtime registry maps each production strategy to one concrete executor. Advertised
capabilities are derived from that registry. Planning fails closed when no registered executor
exists. Registration also declares required request version, evidence version and UAT status.

## Consequences

- Capability/resolver consistency becomes structural rather than documentary.
- Partially implemented strategies are not advertised.
- Tests assert that every advertised capability resolves through the public runtime entry.
