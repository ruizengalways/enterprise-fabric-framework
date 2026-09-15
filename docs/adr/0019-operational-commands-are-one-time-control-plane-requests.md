# ADR 0019: Operational commands are one-time control-plane requests

- Status: Accepted
- Date: 2026-09-15

## Context

Dataset configuration in Git describes durable desired behavior. Operations such as `REBUILD` are
commands intended to run once. Storing such a command in dataset YAML would make every later
schedule, deployment or retry capable of executing it again. A consumed marker committed back to
Git would introduce races, noisy commits and a second operational state store.

The execution mechanism must be easy for a team to use while preserving production approval,
idempotency, frozen inputs and durable evidence.

## Decision

Git contains only durable declarative state:

- dataset identity and contract version;
- capture and load strategies;
- publication, schema, quality and reconciliation policies;
- logical `RelationRef` values;
- environment binding manifests; and
- workflow and deployment definitions.

Git must not contain pending or consumed operational commands, mutable run status, one-time
`REBUILD` flags or production approval state.

`REBUILD` is represented by a bounded, one-time `ExecutionRequest` stored in the SQLAlchemy-backed
control plane. The Fabric Pipeline and Spark Job Definition receive only its opaque
`execution_request_id`. The orchestrator loads, validates and atomically claims the request before
calling the same public Spark runtime used by normal production runs.

## Team-facing workflow

The primary interface is a manually triggered GitHub Actions operational workflow. It presents a
small form rather than requiring developers to edit YAML or operational tables:

- environment;
- logical dataset ID and contract version;
- operation (`REBUILD` initially);
- source selection (`LATEST_COMPLETE_BRONZE` or an explicit boundary);
- reason and incident/change ticket; and
- optional dry-run-only selection.

The workflow has two stages:

1. **Plan:** validate the dataset and permissions, resolve the latest complete Bronze boundary when
   requested, freeze physical bindings/configuration/artifact identity, produce an impact summary
   and create the control-plane request.
2. **Approve and execute:** obtain the protected-environment approval, submit the request ID to one
   generic Fabric operational pipeline, wait for the terminal result and retain evidence.

A CLI may provide the same plan/show/execute/cancel operations for automation and investigation,
but it calls the same control-plane service and does not create a second execution path. No team
member should need to edit the control-plane database directly.

## Request contract

An `ExecutionRequest` contains bounded metadata, never business rows:

- immutable UUID `execution_request_id`;
- environment, dataset ID and contract version;
- operation and requested source selection;
- resolved immutable Bronze Delta version/batch range or equivalent boundary;
- resolved target `RelationRef` identity;
- configuration, binding, plan and wheel hashes;
- reason, ticket, requester and timestamps;
- approval identity/time where policy requires it;
- expiry time;
- state and version for compare-and-swap transitions; and
- references to attempts and retained evidence.

If configuration, bindings, artifact identity or source assumptions change after planning, the
approved plan is stale and execution fails before target mutation. The team creates and reviews a
new request rather than silently refreshing an approved request.

## State and idempotency

The state machine is:

```text
DRAFT -> PLANNED -> AWAITING_APPROVAL -> APPROVED
                                      -> CANCELLED
APPROVED -> CLAIMED -> RUNNING -> SUCCEEDED
                              \-> FAILED
any non-running pre-execution state -> EXPIRED
```

- Claim uses compare-and-swap plus the dataset lease/fencing token.
- A duplicate invocation for `CLAIMED` or `RUNNING` cannot start another execution.
- A duplicate invocation for `SUCCEEDED` returns the existing terminal outcome without mutation.
- `FAILED` is not blindly executed again. An authorized retry creates a new attempt under the same
  request only when its frozen plan is still valid; otherwise a new request is required.
- Every attempt has its own ID and Fabric job identity while retaining the same immutable source
  boundary.
- Ambiguous provider completion is reconciled against Delta/run evidence before any retry.

## Fabric parameter handoff

The generic Fabric Pipeline has one required runtime parameter:

```text
execution_request_id: UUID
```

It passes the value to the Spark Job Definition as a command-line argument. A thin Notebook entry
may use a base parameter only for manual investigation or an explicitly selected notebook runtime;
it must call the same orchestration entry point.

No schedule stores `run_mode=REBUILD`. Normal schedules create or invoke normal run requests through
the standard orchestrator. Production Git deployment may run bounded smoke tests, but cannot create
a production rebuild request merely because files changed.

## Authorization policy

- Dev may allow requester and executor to be the same identity.
- UAT and Prod use protected GitHub Environments.
- Prod requires an explicit approval; organizational policy may require requester and approver to
  be different people.
- The execution service principal has only the Fabric and control-plane permissions required for
  the selected environment.
- Cancellation, retry and expiry are audited actions.

## Evidence

The terminal record binds:

- execution request and attempt IDs;
- GitHub workflow run and approver;
- Git SHA, wheel SHA256 and effective configuration/binding/plan hashes;
- Fabric Pipeline and Spark job instance IDs;
- frozen source boundary and resolved target identity;
- Delta candidate and target commit identities;
- quality/reconciliation result; and
- checkpoint outcome.

## Consequences

- Editing or redeploying repository files cannot repeat a rebuild.
- Operators have one guided workflow with dry-run, approval, status and evidence links.
- Git remains the source of durable configuration, while the control plane is the source of
  operational intent and state.
- The control plane requires request, attempt and state-transition persistence plus lease fencing.
- Unit, Spark and Fabric UAT tests must prove duplicate invocation, stale plan, approval, retry and
  ambiguous-commit behavior.

## References

- [Fabric Pipeline parameters](https://learn.microsoft.com/en-us/fabric/data-factory/parameters)
- [Fabric Spark Job Definition activity](https://learn.microsoft.com/en-us/fabric/data-factory/spark-job-definition-activity)
- [Fabric Notebook run-on-demand parameters](https://learn.microsoft.com/en-us/rest/api/fabric/notebook/background-jobs/run-on-demand-notebook)
