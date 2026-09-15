# ADR 0009: The framework ends at the Silver boundary

- Status: Accepted
- Date: 2026-09-15

## Context

The enterprise has multiple business domains. Each domain owns different dimensional models,
facts, aggregates, KPIs, security rules, semantic models and release cadences. Putting those Gold
concerns in the shared ingestion framework would force domain-specific abstractions into a common
package and recreate the complexity this clean-start repository is intended to avoid.

The shared problem is narrower: capture source facts reliably, preserve their fidelity in Bronze,
and produce governed, reusable Silver datasets with consistent Spark/Delta semantics and
operational evidence.

## Decision

The framework scope is:

```text
enterprise source -> Bronze -> Silver
```

It owns:

- source capture contracts and bounded extraction boundaries;
- Bronze landing modes, identity, manifests, retention hooks and replay evidence;
- Spark-native normalization, validation, quality handling and reconciliation;
- Silver APPEND, REPLACE, UPSERT, SCD1, SCD2, SNAPSHOT_DIFF and reusable current projection;
- checkpoints, leases, run audit, recovery and bounded runtime evidence; and
- logical-to-physical bindings for source, Bronze and Silver relations in Dev, UAT and Prod.

It does not own:

- Gold dimensional models, marts, aggregates or domain KPIs;
- Power BI semantic models, reports or domain serving contracts;
- T-SQL serving views or consumer-facing view repointing;
- domain-specific Gold blue/green deployment or rollback;
- Gold retention and garbage collection; or
- orchestration and deployment of independent domain repositories.

Each domain owns its Gold layer in its own repository. A domain repository consumes a documented
Silver contract and independently chooses its serving, semantic-model and release architecture.

Silver outputs are registered physical Delta tables by default so they are durable and broadly
interoperable across Fabric engines. Spark views may be used for internal or explicitly Spark-only
intermediate relations, but they are not the default cross-domain contract.

## Boundary contract

A Silver dataset contract must expose enough information for a domain repository to consume it
without relying on framework internals:

- logical dataset identity and resolved Fabric item identity;
- Delta relation and schema version;
- business grain and keys;
- current/history and delete semantics;
- source-history fidelity limitations;
- freshness/checkpoint boundary;
- quality and reconciliation status; and
- run and Delta commit evidence.

## Consequences

- REPLACE publication decisions in this repository apply only to stable Silver outputs.
- `RelationRef` resolves source, Bronze and Silver topology; it does not manage Gold aliases or
  semantic-model bindings.
- Gold physical-table and Power BI concerns do not influence the common Spark load strategies.
- Domain teams can evolve Gold independently while sharing one governed Silver foundation.
- End-to-end enterprise tests may use a disposable consumer to verify a Silver contract, but this
  repository does not contain a production Gold implementation.
- A future request to add Gold responsibilities requires a new ADR and evidence that the concern is
  genuinely common across domains.
