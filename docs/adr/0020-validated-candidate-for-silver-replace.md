# ADR 0020: Silver REPLACE uses a validated run-scoped candidate

- Status: Accepted
- Date: 2026-09-15

## Context

A dataset with `load_strategy: REPLACE` receives or derives a complete Silver state under an
unchanged contract. Directly replacing the stable target provides Delta transaction isolation, but
does not provide a durable boundary at which the complete output can be validated before
publication. Creating a permanent versioned table for every refresh confuses data refresh with
contract migration and creates unnecessary lifecycle work.

Breaking contract changes are already handled through parallel v1/v2 contracts under ADR 0018.
One-time rebuild intent is already separated from Git configuration under ADR 0019.

## Decision

The only production publication path for Silver `REPLACE` is:

```text
frozen complete input
  -> run-scoped physical Delta candidate
  -> Spark schema, quality and reconciliation gates
  -> stable physical Silver Delta relation
  -> commit verification
  -> checkpoint commit
```

This is runtime behavior derived from `load_strategy: REPLACE`; it is not another per-dataset
publication switch. Dataset configuration does not need `publication_policy: VALIDATED_CANDIDATE`
while this is the only supported REPLACE executor.

The executor must:

1. acquire the dataset lease/fencing token and freeze the complete source/Bronze boundary;
2. write the transformed result to a unique candidate relation bound to the dataset run ID;
3. validate schema, completeness, required quality rules, uniqueness and reconciliation entirely in
   Spark before mutating the stable target;
4. publish the validated result to the existing stable Silver relation using the Fabric-certified
   Delta replacement primitive;
5. reconcile the observed target commit with run evidence before advancing the checkpoint; and
6. remove successful candidates through idempotent TTL cleanup while retaining failed/ambiguous
   candidates for the diagnostic period.

The candidate is not a new contract version. It is not advertised to domain consumers, has no
independent business identity and cannot be used as a Gold cutover mechanism.

The same validated-candidate publication kernel may be used by an explicitly authorized
same-contract `REBUILD` recovery request. That does not change the dataset's configured load
strategy. Rebuilding an UPSERT/SCD target still reconstructs that target's declared semantics; it
does not silently reconfigure the dataset as normal REPLACE.

## Failure and concurrency semantics

- Failure before publication leaves the stable Silver target and checkpoint unchanged.
- Only one publisher may hold the fenced dataset lease for a stable target.
- Candidate names are unique per run and are never shared mutable staging names.
- An ambiguous publication result is reconciled using Delta commit/run evidence before retry.
- Failure after a proven target commit but before checkpoint commit enters recovery; it does not
  blindly publish the candidate again.
- No source, candidate or target dataset is collected to Python.

## Fabric capability gate

The exact Runtime 2.0 Delta command and its behavior through OneLake/catalog metadata are selected
during implementation. REPLACE is not advertised as production-supported until Fabric UAT proves:

- readers observe a complete old or new stable relation, never a partial result;
- target identity and commit evidence are recoverable;
- failed validation cannot alter the stable target;
- duplicate/ambiguous invocation is safe; and
- the selected rollback operation works within the declared retention window.

## Consequences

- Normal REPLACE runs do not create permanent v2 tables.
- Enterprise validation happens before stable Silver publication.
- Temporary candidate storage and an additional publication step are accepted costs.
- R1 direct replacement and R3 version-per-refresh are not separate production capabilities.
- Tests cover candidate isolation, validation failure, successful publication, checkpoint gating,
  concurrent publishers, replay, ambiguous completion and TTL cleanup.
