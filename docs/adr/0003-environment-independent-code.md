# ADR 0003: Environment-independent code and logical resource bindings

- Status: Accepted
- Date: 2026-09-15

## Decision

Dev, UAT and Prod execute the same versioned wheel and strategy code. Dataset configuration uses
logical resource references. Environment configuration resolves those references to Fabric
workspace, Lakehouse, catalog/schema/table, connection and secret identities.

The stable contract is a typed `RelationRef`, not a physical table-name string. A `RelationRef`
identifies a logical source, Bronze or Silver relation and its intended role. The Fabric resolver
combines it with validated environment bindings and explicit run/deployment context to produce the
engine-specific physical reference required by Spark, Delta, a connector or Fabric API.

Resolved evidence records stable Fabric item identifiers as well as human-readable names and the
schema/table/path components used for that run. Resolution is explicit: CI/CD, Fabric Variable
Libraries and Pipeline parameters may supply binding inputs, but strategy code must not read an
environment name and construct topology itself.

Business behavior cannot branch on an environment name. Intentional operational differences,
such as concurrency or alert destinations, must be explicit typed environment settings.

This decision applies only to source, Bronze and Silver under ADR 0009. `RelationRef` does not own
Gold aliases, semantic-model bindings, consumer cutover, cloning or lifecycle cleanup.

## Consequences

- Promotion changes bindings, not source code.
- Configuration hashes distinguish logical semantics from physical bindings.
- Secrets remain outside source control.
- The resolver may produce different physical address shapes for Spark, SQL endpoints and Fabric
  APIs without leaking those differences into load strategies.
- Missing, ambiguous or cross-environment bindings fail before Spark target mutation.
