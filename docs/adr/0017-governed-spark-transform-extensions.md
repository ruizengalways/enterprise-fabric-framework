# ADR 0017: Transform extensions are governed and Spark-native

- Status: Accepted
- Date: 2026-09-15

## Context

Some Bronze-to-Silver transformations require joins or business mappings that cannot be expressed
comfortably as simple declarative configuration. Arbitrary callbacks would allow driver collection,
undeclared inputs, external side effects and hidden execution paths.

Gold transformations are outside this repository under ADR 0009. Designing a shared plugin system
for domain Gold repositories would expand the framework beyond its product boundary.

## Decision

The architecture adopts registered Spark-native extensions alongside declarative Spark rules.
Extensions are only for in-scope source-to-Bronze or Bronze-to-Silver work. They must:

- be resolved by stable registered name and version;
- accept and return Spark DataFrames or declared relation plans, never row lists or pandas objects;
- declare additional input/output `RelationRef` values and required capabilities;
- run inside the normal plan, run ID, evidence, quality and reconciliation boundary;
- be deterministic for the same declared inputs unless explicitly classified otherwise;
- avoid unbounded driver collection and undeclared external side effects; and
- contribute their identity/version to the plan hash and run evidence.

The first implementation phase keeps `extensions/` as a placeholder and does not publish a callback
API. The concrete protocol and registry are introduced only when a real Silver use case cannot be
served by the declarative transform layer. That implementation requires contract tests, a static
boundary check and at least one production-path Spark integration test.

Gold repositories do not receive a Gold plugin framework from this package. They own their domain
code and may consume stable Silver contracts. Any future cross-domain transformation SDK is a
separate product decision.

## Consequences

- The direction is fixed without prematurely freezing an extension API.
- Declarative Spark transforms are implemented first.
- No empty generic callback abstraction becomes a compatibility burden.
- A future extension implementation cannot bypass the public Spark runtime or become another apply
  engine.
