# Architecture decision records

ADRs begin as `Proposed`. After review they become `Accepted`, `Rejected` or `Superseded`.

- [0001: Spark and Delta are the only production business-data runtime](0001-spark-delta-only-production-runtime.md)
- [0002: One runtime registry is the capability source of truth](0002-runtime-registry-is-capability-source.md)
- [0003: Environment-independent code and logical resource bindings](0003-environment-independent-code.md)
- [0004: Bounded driver evidence](0004-bounded-driver-evidence.md)
- [0005: Utility package boundaries](0005-utility-package-boundaries.md)
- [0006: GitHub Actions controls CI/CD](0006-github-actions-controls-cicd.md)
- [0007: Test the production Spark path](0007-test-production-spark-path.md)
- [0008: Clean start without legacy compatibility](0008-clean-start-without-legacy-compatibility.md)
- [0009: The framework ends at the Silver boundary](0009-framework-ends-at-silver.md)
- [0010: Governed per-dataset SCD2 late-arrival policy](0010-governed-scd2-late-arrival-policy.md)
- [0011: CDC delete semantics are explicit per dataset](0011-explicit-cdc-delete-semantics.md)
- [0012: Fabric Copy is permitted as Bronze ingress only](0012-fabric-copy-is-bronze-ingress-only.md)
- [0013: Bronze representation is explicit per dataset](0013-explicit-bronze-representation.md)
- [0014: APPEND and event ingestion require explicit identity](0014-explicit-event-identity.md)
- [0015: Schema evolution is governed and compatibility-aware](0015-governed-schema-evolution.md)
- [0016: Pin Fabric Runtime 2.0 as the initial baseline](0016-pin-fabric-runtime-2-0.md)
- [0017: Transform extensions are governed and Spark-native](0017-governed-spark-transform-extensions.md)
- [0018: Breaking Silver changes use parallel contract versions](0018-parallel-silver-contract-migration.md)
- [0019: Operational commands are one-time control-plane requests](0019-operational-commands-are-one-time-control-plane-requests.md)
- [0020: Silver REPLACE uses a validated run-scoped candidate](0020-validated-candidate-for-silver-replace.md)
