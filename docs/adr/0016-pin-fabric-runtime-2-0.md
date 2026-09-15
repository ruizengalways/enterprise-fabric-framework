# ADR 0016: Pin Fabric Runtime 2.0 as the initial baseline

- Status: Accepted
- Date: 2026-09-15

## Context

The framework needs one reproducible production and CI baseline. At the time of this decision,
Microsoft identifies Fabric Runtime 2.0 as the latest generally available runtime. A request to use
the latest runtime means selecting and pinning the latest GA baseline at a reviewed point in time;
it must not make builds float whenever Microsoft changes the platform default.

## Decision

The initial production baseline is:

| Component | Baseline |
|---|---|
| Microsoft Fabric Runtime | 2.0 GA |
| Apache Spark | 4.1 |
| Delta Lake | 4.2 |
| Python | 3.13 |
| Java | 21 |
| Scala | 2.13 |

Dev, UAT and Prod Fabric Environments must explicitly select Runtime 2.0. Local development and
GitHub Actions use the closest available pinned open-source patch versions for the same Spark,
Delta, Python and Java lines. The exact resolved image manifest and wheel lock are recorded once the
test image is built.

Runtime upgrades are deliberate changes: create a candidate baseline, run the local suite, certify
the exact wheel in Fabric UAT, then update this ADR or supersede it. The project does not track
`latest` dynamically.

Silver tables must remain interoperable with their expected Fabric consumers. Delta 4.2 features
that are experimental or Spark-only are disabled unless a separate capability decision and UAT
prove that every declared consumer can read the resulting table protocol.

## Consequences

- Package metadata and tooling target Python 3.13.
- CI image work targets Java 21, Spark 4.1 and Delta 4.2 rather than the former placeholder baseline.
- Fabric-native differences still require real Fabric UAT; matching version labels do not make
  local Spark a Fabric emulator.
- A tenant that cannot enable Runtime 2.0 cannot certify this package until an explicit alternative
  baseline is accepted.
