"""Run every frozen Delta Sharing Bronze delivery in one execution group.

Run this module as a Fabric Job Definition with ``CONTROL_PLANE_SQLALCHEMY_URL``,
``DELTA_SHARING_PROFILE``, ``PIPELINE_RUN_ID``, and
``DELTA_SHARING_RUNTIME_FACTORY_BUILDER=package.module:function``. The builder receives the
Spark session, Delta Sharing profile, and environment mapping, then returns the project's
``DeltaSharingTableRuntimeFactory``. That factory is called separately for each frozen table
delivery and resolves the table's ingestion, DQ, and reconciliation references.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from importlib import import_module
from typing import Any, Callable

from enterprise_fabric_framework.orchestration import (
    ExecutionGroupOutcome,
    TableDeliveryAuditManager,
)
from enterprise_fabric_framework.producer.delta_sharing_job import (
    DeltaSharingExecutionGroupJob,
    DeltaSharingTableRuntimeFactory,
)

RuntimeFactoryBuilder = Callable[
    [Any, str, Mapping[str, str]],
    DeltaSharingTableRuntimeFactory,
]


def ingest_execution_group(
    control_plane_connection_url: str,
    runtime_factory: DeltaSharingTableRuntimeFactory,
    environment: Mapping[str, str] = os.environ,
    *,
    audit_manager: TableDeliveryAuditManager | None = None,
) -> ExecutionGroupOutcome:
    """Run every frozen table in ``PIPELINE_RUN_ID`` with per-delivery components."""

    return DeltaSharingExecutionGroupJob(
        control_plane_connection_url,
        runtime_factory,
        audit_manager=audit_manager,
    ).run(environment)


def runtime_factory_from_environment(
    spark: Any,
    environment: Mapping[str, str] = os.environ,
) -> DeltaSharingTableRuntimeFactory:
    """Build the deployment-specific factory configured for this Fabric job."""

    builder = _runtime_factory_builder(
        _required(environment, "DELTA_SHARING_RUNTIME_FACTORY_BUILDER")
    )
    runtime_factory = builder(
        spark,
        _required(environment, "DELTA_SHARING_PROFILE"),
        environment,
    )
    if not isinstance(runtime_factory, DeltaSharingTableRuntimeFactory):
        raise TypeError(
            "DELTA_SHARING_RUNTIME_FACTORY_BUILDER must return a "
            "DeltaSharingTableRuntimeFactory"
        )
    return runtime_factory


def main() -> None:
    """Run the execution group selected by ``PIPELINE_RUN_ID`` in Fabric Spark."""

    from pyspark.sql import SparkSession

    environment = os.environ
    outcome = ingest_execution_group(
        _required(environment, "CONTROL_PLANE_SQLALCHEMY_URL"),
        runtime_factory_from_environment(SparkSession.builder.getOrCreate(), environment),
        environment,
    )
    print(
        "Bronze execution group completed "
        f"for {outcome.pipeline_run_id} with {outcome.status.value}."
    )


def _runtime_factory_builder(reference: str) -> RuntimeFactoryBuilder:
    module_name, separator, attribute_name = reference.partition(":")
    if not separator or not module_name or not attribute_name:
        raise ValueError(
            "DELTA_SHARING_RUNTIME_FACTORY_BUILDER must use 'package.module:function'"
        )
    builder = getattr(import_module(module_name), attribute_name, None)
    if not callable(builder):
        raise TypeError(
            "DELTA_SHARING_RUNTIME_FACTORY_BUILDER must identify a callable factory builder"
        )
    return builder


def _required(environment: Mapping[str, str], variable_name: str) -> str:
    value = environment.get(variable_name)
    if value is None or not value.strip():
        raise ValueError(f"{variable_name} must be set")
    return value


if __name__ == "__main__":
    main()