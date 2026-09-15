# ADR 0007: Test the production Spark path

- Status: Proposed
- Date: 2026-09-15

## Context

Business-data processing runs in Microsoft Fabric Spark. A pure-Python implementation would create
a second semantic system, while passing local tests alone cannot prove OneLake, Fabric catalog,
managed identity, Job Definition, Pipeline or managed-runtime behavior.

The framework needs fast pull-request feedback, reproducible local development and real-platform
release evidence without duplicating APPEND, REPLACE, UPSERT, SCD1, SCD2, SNAPSHOT_DIFF, CDC or
projection algorithms.

## Decision

Testing uses one production Spark implementation across several execution environments.

1. Python unit tests cover only bounded non-business-data concerns such as configuration,
   contracts, planning, orchestration, control-plane state, audit and Fabric protocol handling.
2. GitHub Actions starts local Spark/Delta and runs business-semantic tests through the same public
   `SparkDatasetRuntime.run()` entry used in Fabric.
3. A pinned Docker image standardizes Java, Python, Spark and Delta versions for developers and
   GitHub Actions. Docker is an execution environment, not another test implementation.
4. Real Fabric UAT executes the exact candidate wheel through a Spark Job Definition. It validates
   OneLake, Lakehouse catalogs, Environment libraries, identity, bindings, retries and platform
   concurrency before production approval.
5. Fabric notebooks are diagnostic tools or thin manual launchers. They are not the primary
   automated test runner and must not contain business algorithms.

Local Spark and real Fabric prove different risks. Local success is required for fast correctness
feedback; Fabric UAT is required for production certification.

## Test data and assertions

Small deterministic fixtures may use `spark.createDataFrame`. Larger synthetic datasets use
Spark-native generators such as `spark.range` and Spark expressions. Tests must not build large
Python lists or pandas frames.

Expected outcomes are verified with fixed expected relations, bidirectional `EXCEPT ALL`, Delta
history and semantic invariants. The test suite does not calculate expected results with a second
Python business engine and does not contain Python-versus-Spark parity tests.

## Isolation and cleanup

Each integration run receives a unique run ID and isolated tables or schemas. Failed Fabric UAT
state is retained for investigation and removed later by a TTL cleanup job. Cleanup is never part
of the correctness assertion and must not erase failure evidence before it is retained.

## Execution cadence

- Pull request: Python unit tests and the fast local Spark/Delta suite.
- Main branch: full local Spark/Delta suite and exact-wheel construction.
- Nightly: replay, randomized batches, failure injection, concurrency and larger volumes.
- Release candidate: exact-wheel Fabric UAT and retained evidence.
- Production deployment: bounded read-only or idempotent smoke checks.

## Consequences

- CI exercises production Spark code instead of an in-memory substitute.
- Developers and GitHub Actions can share one reproducible container image.
- Fabric test cost and latency are reserved for the risks only Fabric can prove.
- Spark/Delta compatibility versions must be pinned after the supported Fabric runtime is selected.
- UAT automation needs isolated test resources, retained evidence and governed cleanup.

## References

- [Microsoft Fabric CI/CD overview](https://learn.microsoft.com/en-us/fabric/cicd/cicd-overview)
- [Create a Spark Job Definition](https://learn.microsoft.com/en-us/fabric/data-engineering/create-spark-job-definition)
- [Fabric Environment compute](https://learn.microsoft.com/en-us/fabric/data-engineering/environment-manage-compute)
