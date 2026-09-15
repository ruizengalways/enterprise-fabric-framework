# ADR 0010: Governed per-dataset SCD2 late-arrival policy

- Status: Accepted
- Date: 2026-09-15

## Context

SCD2 sources differ in history fidelity and lateness guarantees. A reliable effective timestamp can
support interval correction only when entity identity and ordering are also deterministic. A single
global policy would either reject valid enterprise corrections or silently rewrite history for
datasets that cannot prove it.

## Decision

Every SCD2 dataset that can receive late records declares one of two policies:

- `REJECT_AND_REBUILD`: incremental processing does not rewrite established history. A late record
  produces bounded evidence and a rebuild-required outcome.
- `CORRECT_WITHIN_WINDOW`: the Spark runtime may correct affected-key intervals within a strictly
  positive configured duration. A record outside the window produces a rebuild-required outcome.

`REJECT_AND_REBUILD` is the default fail-closed policy. A zero duration is not used as a magic value
for another mode. Unrestricted retroactive correction is not supported.

`CORRECT_WITHIN_WINDOW` is valid only when metadata declares and validation proves:

- a stable business key;
- authoritative business effective time rather than ingestion/refresh time;
- a deterministic tie-breaker for equal effective timestamps;
- source fidelity sufficient for the claimed history;
- explicit delete and correction semantics; and
- a duration and comparison boundary with unambiguous timezone handling.

Correction runs operate entirely in Spark over affected keys and must revalidate non-overlapping
intervals, positive interval length and at most one current row per key. The runtime never collects
affected keys or history to Python.

## Consequences

- Dataset owners choose safety versus bounded automation explicitly.
- Outside-window data is preserved in Bronze and evidenced, but is not silently discarded or
  applied as the current row.
- Same-effective-time conflicts fail unless the configured ordering contract resolves them.
- Rebuild remains an explicit recovery workflow, not an alternate in-memory apply path.
- Local Spark and Fabric UAT must cover inside-window correction, outside-window rejection, replay,
  equal timestamps and temporal invariants.
