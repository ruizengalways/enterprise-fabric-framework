# ADR 0008: Clean start without legacy compatibility

- Status: Accepted
- Date: 2026-09-15

## Context

`enterprise-fabric-framework` is a new repository created because incrementally restructuring the
previous `fabric-data-framework` had become too complex. Treating the new package as a compatible
upgrade would preserve the old abstractions, duplicate paths and migration constraints that the
clean start is intended to remove.

The old repository remains useful as evidence of real data patterns, operational requirements and
past failure modes. It is not the contract for the new runtime.

## Decision

The new framework provides no compatibility with the previous package:

- no legacy import paths, facade modules or runtime shims;
- no legacy configuration parser or automatic configuration conversion;
- no fallback to old `apply`, execution, adapter or certification paths;
- no parity suite requiring old and new engines to produce the same result;
- no release or API-version promise linking the two repositories; and
- no dependency on the old package in production, tests or deployment.

Useful business concepts may be re-specified and implemented natively when they fit the accepted
Spark-first architecture. Copying a concept does not imply preserving its API, behavior by
accident, directory layout or implementation.

Migration of a particular deployed dataset, table or checkpoint is a separately planned delivery
activity. If ever required, it must use explicit one-time migration tooling and reconciliation;
it must not introduce a permanent compatibility layer into the framework.

## Consequences

- New contracts can be small and internally consistent.
- Dataset adoption is an explicit migration, not a drop-in package upgrade.
- Documentation must not instruct users to mix old and new runtime components.
- The old repository may be consulted during design, but code is not copied merely to preserve
  compatibility.
- Any future request for compatibility requires a new ADR and a concrete business justification.
