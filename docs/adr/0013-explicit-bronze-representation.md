# ADR 0013: Bronze representation is explicit per dataset

- Status: Accepted
- Date: 2026-09-15

## Context

Complete snapshots, watermark observations, ordered CDC and business events expose different
facts. Treating them all as one append log can manufacture history, while always retaining every
capture indefinitely creates unnecessary cost and governance exposure.

## Decision

Every dataset declares exactly one Bronze representation compatible with its source contract:

- `SNAPSHOT`: an immutable observation of complete state at a proven boundary;
- `EVENT_LOG`: immutable records with stable event/change identity and ordering semantics;
- `CURRENT_STAGE`: a governed current-state staging relation when history is not claimed; or
- `EPHEMERAL`: a non-durable handoff allowed only by explicit retention/recovery exception.

There is no implicit default. Plan compilation fails when the mode is missing or incompatible with
capture facts, load semantics, replay requirements or retention policy.

A watermark observation is not automatically an event, a full scan is not automatically a change
log and absence is not a delete unless snapshot completeness is proven.

## Consequences

- Bronze preserves the truth ceiling of the source.
- Durable `SNAPSHOT` and `EVENT_LOG` are the normal enterprise choices.
- `CURRENT_STAGE` documents limited replay/history behavior.
- `EPHEMERAL` requires an explicit exception and cannot support capabilities that require durable
  replay evidence.
- Storage retention remains policy-driven rather than being embedded in load algorithms.
