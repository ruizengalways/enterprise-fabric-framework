---
id: examples.thin-spark-runner
status: illustrative
last_reviewed: 2026-09-18
---

# Thin Spark runner boundary

The Fabric schedule and Pipeline belong to the domain repository. The framework package exposes
the runner boundary only; it does not create schedules or workspace items.

## Scheduled handoff

```text
Fabric schedule
  -> domain Fabric Pipeline
  -> control.usp_plan_execution_group(...)
  -> frozen bronze_run_id / silver_run_id
  -> Spark Job Definition parameters
  -> domain thin launcher
  -> enterprise_fabric_framework.platform.fabric invocation adapter
  -> DatasetRunner
  -> SparkDatasetRuntime
```

## Spark Job Definition parameters

Silver consumer invocation:

```text
request_schema_version=1
silver_run_id=<opaque frozen control-plane run ID>
execution_request_id=<optional approved rebuild/recovery request ID>
```

Source-to-Bronze invocation:

```text
request_schema_version=1
bronze_run_id=<opaque frozen control-plane run ID>
execution_request_id=<optional approved recovery request ID>
```

The launcher does not receive business rows, policies, strategy names, physical table paths or
checkpoint paths. It forwards explicit IDs; the control-plane adapter loads the frozen plan.

The package's default control-plane schema will be provisioned from templates under
`sql/control_plane/`. A domain with an existing control plane composes its adapter in the runner
and injects it at startup. Storage details are not passed through the Spark Job.

`runner.run(request)` returns a bounded execution result to the Job/Notebook. During execution, the
runtime records lifecycle and evidence through the injected control-plane adapter. The returned
result and persisted control-plane state are related, but they are not the same operation.

## Function-shaped launcher

The domain-owned launcher is intentionally small. A complete illustrative Python shape is in
[`thin_spark_runner.py`](thin_spark_runner.py):

```python
from collections.abc import Mapping


def main(raw_parameters: Mapping[str, str]) -> int:
    invocation = normalize_invocation(raw_parameters)
    request = to_spark_request(invocation)
    result = build_runner().run(request)
    return 0 if result.status == "SUCCEEDED" else 1
```

The functions above are interface shapes only. The framework's production path is
`DatasetRunner -> SparkDatasetRuntime`; a domain launcher must not implement transformation,
quality, loading, reconciliation or control-table writes.

Schedules, trigger times, retries and Pipeline activity wiring remain Fabric/domain-owned. The
package receives the frozen run identity after planning, so a schedule change does not change the
data contract or the Spark algorithm.
