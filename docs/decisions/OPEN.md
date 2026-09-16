---
id: decisions.open
status: open
last_reviewed: 2026-09-16
---

# Open architecture decisions

Only unresolved choices that change runtime contracts belong here. After acceptance, update the
canonical architecture and remove the resolved entry. Delete this file when no open decision
remains.

## OD-01: Capture identity and Silver consumer fan-out

### Conflict

Current manifest wording can bind a capture to `dataset_run_id`, but parallel Silver v1/v2 requires
two independently frozen consumer runs to reuse the same authoritative Bronze boundary.

### Options

- `OD-01-A`: One capture belongs to one end-to-end dataset run; duplicate capture per consumer.
- `OD-01-B` — recommended: use an independent immutable `capture_run_id`/`capture_id`; each Silver
  `dataset_run_id` references one compatible completed capture.
- `OD-01-C`: Bind one capture to the whole Pipeline/execution-group run.

### Owner response

`UNDECIDED`

## OD-02: CURRENT_STAGE minimum replay retention

### Conflict

CURRENT_STAGE pins a Delta version, but ownership of the minimum period protecting its data files
from cleanup/VACUUM is not defined.

### Options

- `OD-02-A` — recommended: an organization minimum retry-retention floor; datasets may retain longer
  but not shorter, and active capture leases prevent cleanup.
- `OD-02-B`: entirely dataset-specific retention.
- `OD-02-C`: recapture on retry, accepting a changed input boundary.

### Owner response

`UNDECIDED`

## OD-03: EPHEMERAL Bronze in the first release

### Conflict

EPHEMERAL is reserved in the representation contract but weakens normal retry/rebuild behavior and
adds an exception path before durable writers exist.

### Options

- `OD-03-A` — recommended: reserve the value but do not register a writer in the first release.
- `OD-03-B`: remove it until a real requirement exists.
- `OD-03-C`: implement it initially with approval, no-replay evidence and restricted strategies.

### Owner response

`UNDECIDED`
