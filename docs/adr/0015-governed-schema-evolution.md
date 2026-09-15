# ADR 0015: Schema evolution is governed and compatibility-aware

- Status: Accepted
- Date: 2026-09-15

## Context

Enterprise sources regularly add fields, while type narrowing, removal, rename, nullability and key
changes can corrupt Silver contracts or break downstream domain repositories. Global automatic
Delta schema merge proves storage compatibility, not business compatibility.

## Decision

Each dataset selects a governed schema policy. The framework supports:

- `STRICT`: every unapproved schema difference fails; and
- `ADDITIVE_NULLABLE`: explicitly permitted nullable additions may be accepted while breaking
  changes fail or enter a configured quarantine path.

`STRICT` is the fail-closed default. Automatic schema merge is never enabled globally. Type
narrowing, incompatible type changes, column removal/rename, nullability tightening and any
business/event key change require a reviewed contract version; they cannot be auto-applied.

Every detected schema difference is computed in Spark or from bounded schema metadata and recorded
in run evidence. Bronze may preserve newly observed source fields according to its capture contract,
but propagation into the stable Silver schema follows the Silver dataset policy.

## Consequences

- Regulated datasets can reject all unapproved drift.
- Routine nullable additions can be enabled deliberately without weakening key contracts.
- Downstream domain repositories receive a versioned Silver schema rather than accidental drift.
- Tests cover accepted additions, each breaking category, quarantine behavior and replay after an
  approved schema release.
