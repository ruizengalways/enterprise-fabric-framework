# Dataset rebuild runbook

This is the planned team workflow defined by ADR 0019. Commands and workflow names are interface
placeholders until the control-plane implementation is built.

## What a rebuild is

A rebuild regenerates an existing Silver contract from an explicitly frozen, authoritative source
or Bronze boundary. It does not change `load_strategy`, does not create a breaking v2 contract and
is not enabled by editing dataset YAML.

Use parallel contract migration under ADR 0018 instead when schema, grain, keys or business
semantics change incompatibly.

## Before requesting

Confirm:

- the dataset and contract version to rebuild;
- whether retained Bronze can reproduce the required result;
- the intended boundary or latest known complete Bronze batch;
- the incident/change ticket and business reason;
- the expected affected Silver relation; and
- whether downstream scheduling should be paused during publication.

Do not edit `run_mode` into a dataset or environment file. Do not update the control-plane database
by hand.

## Preferred GitHub workflow

1. Open **Actions -> Dataset operation**.
2. Select **Run workflow**.
3. Enter environment, dataset ID, contract version, `REBUILD`, source selection and ticket.
4. Leave **dry run** enabled for the first submission.
5. Review the generated plan summary:
   - exact source boundary;
   - resolved Bronze and Silver identities;
   - configuration/binding/wheel hashes;
   - current checkpoint and proposed action;
   - expected publication mode; and
   - warnings or unsupported recovery conditions.
6. Submit the planned request for the required environment approval.
7. After approval, execute the same immutable request ID.
8. Follow the GitHub link to Fabric Pipeline/Spark job status and retained evidence.
9. Confirm terminal reconciliation and checkpoint outcome before resuming dependent schedules.

The production workflow never runs a request that has not completed the plan and approval stages.

## Pipeline handoff

GitHub passes only:

```text
execution_request_id=<uuid>
```

The Fabric Pipeline forwards it to the production Spark Job Definition:

```text
--execution-request-id <uuid>
```

The runner resolves the request from the control plane, validates that the approved hashes and
boundaries are still current, claims it once and executes the production Spark path.

## Retry guidance

- If planning or approval fails, correct the input and create a new request.
- If execution fails before mutation, use the authorized retry action only when the frozen plan is
  still valid.
- If the Fabric result is ambiguous, do not click retry immediately. Allow reconciliation to check
  Delta commit and run evidence first.
- If source boundary, code, configuration or bindings must change, create and approve a new request.
- Reusing a successful request ID is a no-op and returns its existing result.

## Completion evidence

A rebuild is complete only when the operation record shows:

- `SUCCEEDED` terminal state;
- matching Git/wheel/configuration/binding identities;
- exact source boundary;
- target Delta commit identity;
- passed required quality and reconciliation;
- correct checkpoint decision; and
- GitHub and Fabric run links.

Pipeline success without the framework terminal outcome is not sufficient.

## Cancellation and cleanup

Cancel only through the GitHub workflow or future CLI so the state transition is audited. Candidate
data is retained for the configured diagnostic TTL on failure and cleaned by an idempotent lifecycle
job. Rebuild execution does not perform ad hoc DROP or VACUUM.
