# ADR 0014: APPEND and event ingestion require explicit identity

- Status: Accepted
- Date: 2026-09-15

## Context

Entity identity, event identity and source progress are often different. Automatically hashing a
whole row can collapse two legitimate identical business events, while technical ingestion columns
can cause the same source event to receive a different hash on replay.

## Decision

APPEND and event-like capture require an explicit identity contract. It may use:

- a stable source-native event/change identifier; or
- a documented composite identity whose columns and null semantics are versioned.

A canonical content hash may be used as identity only when the source/business contract explicitly
states that identical canonical observations represent the same event. It is never an automatic
fallback.

An exact replay of an existing identity and payload is a no-op. Reuse of the same identity with a
different canonical payload fails closed and produces conflict evidence. Entity key, event identity
and cursor/checkpoint remain separate contract fields even when a particular source uses the same
column for more than one role.

Business-row hashes are Spark expressions over declared columns. Hash algorithm, canonicalization
version and participating columns are part of the plan/evidence; ingestion metadata is excluded
unless explicitly part of business identity.

## Consequences

- APPEND is replay-safe without assuming whole-row equality means event equality.
- Missing identity fails plan compilation.
- Change-detection row hashes and event-identity hashes have distinct purposes and contracts.
- Replay and conflicting-identity cases require Spark/Delta tests and Fabric UAT.
