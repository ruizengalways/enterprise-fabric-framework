# ADR 0018: Breaking Silver changes use parallel contract versions

- Status: Accepted
- Date: 2026-09-15

## Context

A corrected or redesigned Silver dataset can change schema, grain, keys, history fidelity or
business semantics while existing domain repositories still depend on the current contract.
Changing v1 in place would force synchronized consumer migration and prevent safe comparison or
rollback.

This is different from routine REPLACE of data under an unchanged Silver contract.

## Decision

Breaking Silver changes use parallel, independently addressable contract versions. V1 remains
available while v2 is built, validated and adopted by domain repositories.

Each version has:

- a stable dataset family and immutable `contract_version`;
- independently resolved Bronze/Silver `RelationRef` values where required;
- independent target, checkpoint/projection and run state;
- declared upstream Bronze contract version and source boundary;
- lifecycle status and replacement relationship; and
- bounded build, quality and reconciliation evidence.

When Bronze is valid and durable, Silver v1 and v2 consume the same captured Bronze evidence. The
framework bootstraps v2 to a frozen boundary and then fans subsequent Bronze batches to both Silver
versions during the migration window.

When Bronze is also incorrect, the v1 chain remains unchanged and the framework creates separately
bound Bronze v2 and Silver v2 relations. V2 is re-extracted or rebuilt from authoritative evidence
using an explicit no-gap baseline/incremental handoff. The framework does not claim history that the
source or retained evidence cannot reconstruct.

Gold and reporting cutover remain outside this framework. Each domain repository validates the
published Silver v2 contract and changes its own binding. V1 processing is paused only after
consumer migration approval. V1 data remains readable for a governed rollback/retention period and
is later deleted by a separately authorized cleanup workflow.

V2 content is not written over a relation identified as v1. A contract version describes semantics,
not merely a temporary table name.

## Consequences

- Existing reports and Gold repositories are not disturbed while v2 is validated.
- Parallel execution consumes extra capacity and is limited to an explicit migration window.
- Consumer inventory and migration approval are governance inputs; a successful Spark run alone
  cannot declare v2 active or v1 retired.
- Pausing compute and deleting data are separate operations.
- Shallow clone may accelerate short-lived validation, but cannot be a durable migration dependency
  unless source-file retention and VACUUM dependencies are governed.
- Spark tests and Fabric UAT must cover shared-Bronze fan-out, full Bronze/Silver fork, checkpoint
  isolation, catch-up, rollback and retirement guards.
- Bootstrap, rebuild and catch-up execution uses the one-time control-plane request model in ADR
  0019; no migration operation flag is committed to dataset configuration.
