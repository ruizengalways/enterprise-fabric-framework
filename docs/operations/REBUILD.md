---
id: operations.rebuild
status: current
last_reviewed: 2026-09-17
---

# Dataset rebuild runbook

Commands and workflow names remain interface placeholders until the control-plane implementation is
built. The required behavior is normative.

## What a rebuild is

A rebuild regenerates an existing compatible Silver contract through the same Structured Streaming
runtime from an explicitly frozen, authoritative source or Bronze selection. It does not change
`load_strategy`, create a breaking v2 contract or become enabled by editing durable metadata.

Use parallel contract migration in `../architecture/DATA_LIFECYCLE.md` when grain, keys, schema or
semantics change incompatibly.

## Before requesting

Confirm:

- dataset and contract version;
- whether retained Bronze can reproduce the required result;
- exact intended source boundary;
- incident/change ticket and reason;
- affected Silver relation; and
- whether dependent schedules should pause.

Do not add a one-time command to domain metadata SQL or edit operational tables by hand.

## Preferred workflow

1. Open the protected GitHub **Dataset operation** manual workflow.
2. Enter environment, dataset, contract, `REBUILD`, source selection and ticket.
3. Run dry-run planning first.
4. Review source boundary, resolved relations, wheel/configuration/binding hashes, checkpoint,
   publication mode and warnings.
5. Obtain the required environment approval.
6. Execute the same immutable request ID.
7. Follow its Fabric Pipeline/Spark links and retained evidence.
8. Confirm reconciliation and checkpoint handoff, then explicitly restore intended normal enablement
   and resume dependants.

Production MUST NOT execute an unplanned or unapproved request.

## Replay input and checkpoint

Before execution, disable the affected normal configuration through its public enablement procedure,
then explicitly stop any running normal query and acquire exclusive target/query ownership. See
[Enablement and retirement](../architecture/CONTROL_PLANE.md#enablement-and-retirement).
Preserve its checkpoint until recovery no longer needs it; do not reset the active checkpoint in place.

The request freezes a readable Bronze version and optional cutoff predicate, including the date
column, timezone and inclusivity when applicable. Preparation uses Spark/Delta to materialize that
selection into a finite immutable replay relation. This preparation is part of the public recovery
path, not a separate load algorithm. The replay query reads that relation with `AvailableNow` and
a fresh request-specific checkpoint, builds a candidate target, and publishes only after the full
input has drained and required final validation has passed. Micro-batch writes also use the normal
replay-safe executors.

`AvailableNow` alone does not freeze a caller-selected historic cutoff on a changing source. A
registered replay capability MUST prove the requested selection and evidence before execution.
An unreadable boundary fails closed. The completion plan MUST prove a no-gap/no-duplicate handoff
between the rebuilt target and resumed normal consumption before schedules restart. It MUST NOT
attach an old checkpoint blindly to a rebuilt target or restart a full replay into it. Keep the
replay/candidate state until that handoff is verified.

## Pipeline handoff

GitHub passes only:

```text
execution_request_id=<uuid>
```

The Fabric Pipeline forwards it to the production launcher. The runner validates approved hashes
and boundaries, claims it once and invokes the public production path.

## Retry guidance

- Planning/approval failure requires a corrected new request.
- A pre-mutation execution failure may use authorized retry while the frozen plan remains valid.
- An ambiguous Fabric result requires Delta/evidence reconciliation before retry.
- Changed code, configuration, binding or source boundary requires a new request.
- Reusing a successful request returns its existing result as a no-op.

## Completion evidence

A rebuild is complete only with:

- terminal `SUCCEEDED` control-plane state;
- matching Git, wheel, configuration and binding identities;
- exact source boundary;
- target Delta commit identity;
- passed required quality and reconciliation;
- replay checkpoint identity and proven normal-query handoff; and
- GitHub/Fabric run references.

Pipeline success alone is insufficient.

## Cancellation and cleanup

Cancel through the governed workflow or CLI so the state transition is audited. Failed candidate
data is retained for its diagnostic TTL and removed by an idempotent lifecycle job. Rebuild
execution MUST NOT perform ad hoc DROP or VACUUM.
