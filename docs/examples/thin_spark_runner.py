"""Illustrative domain-owned thin launcher for a Fabric Spark Job or Notebook.

This file is an example of the code that belongs in a domain repository. It shows the invocation
boundary only; it does not implement Spark transformations, load strategies or control-table SQL.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping

from enterprise_fabric_framework.control_plane import ControlPlanePort
from enterprise_fabric_framework.orchestration import DatasetRunner
from enterprise_fabric_framework.platform.fabric import normalize_invocation, to_spark_request
from enterprise_fabric_framework.spark import SparkDatasetRuntime, SparkRuntimeDependencies
from enterprise_fabric_framework.spark.registry import CapabilityRegistry


def build_control_plane() -> ControlPlanePort:
    """Compose the deployment's default or company-owned control-plane adapter."""

    # The deployment wires its selected adapter here. The package's default SQL templates are
    # provisioned separately in the target SQL Database; an existing control plane can be injected
    # from the domain repository instead.
    raise NotImplementedError("Wire the deployment control-plane adapter")


def build_registry() -> CapabilityRegistry:
    """Compose the installed, certified Spark capability registry."""

    raise NotImplementedError("Provide the deployment's certified capability registry")


def build_runner() -> DatasetRunner:
    """Build the public runner once at the launcher composition root."""

    runtime = SparkDatasetRuntime(
        SparkRuntimeDependencies(
            control_plane=build_control_plane(),
            registry=build_registry(),
        )
    )
    return DatasetRunner(runtime)


def main(raw_parameters: Mapping[str, str]) -> int:
    """Run one opaque frozen Bronze or Silver plan and return a process exit code."""

    invocation = normalize_invocation(raw_parameters)
    request = to_spark_request(invocation)
    result = build_runner().run(request)

    if result.status != "SUCCEEDED":
        raise RuntimeError(f"Framework run failed: {result.error}")
    return 0


def spark_job_main() -> int:
    """Spark Job Definition entry point using explicit command-line parameters."""

    parser = argparse.ArgumentParser()
    parser.add_argument("--request-schema-version", required=True)
    parser.add_argument("--bronze-run-id")
    parser.add_argument("--silver-run-id")
    parser.add_argument("--execution-request-id")
    args = vars(parser.parse_args())
    parameters = {key: value for key, value in args.items() if value is not None}
    return main(parameters)


def fabric_notebook_main(notebook_parameters: Mapping[str, str]) -> int:
    """Notebook entry point; Pipeline parameters are passed in unchanged."""

    return main(notebook_parameters)


if __name__ == "__main__":
    raise SystemExit(spark_job_main())
