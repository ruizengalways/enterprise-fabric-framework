"""Abstract Fabric job composition for an execution group of table deliveries."""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from collections.abc import Mapping

from .table_delivery import (
    ExecutionGroupOutcome,
    ExecutionGroupRunner,
    TableDeliveryAuditManager,
    TableDeliveryExecutor,
)


class ExecutionGroupJob(ABC):
    """Customizable job base that runs every frozen delivery in one pipeline invocation.

    A deployment subclasses this type to bind Fabric credentials, a control-plane adapter, and a
    runtime factory. The base loads the parent run ID and delegates the explicit table loop to
    ``ExecutionGroupRunner``.
    """

    @abstractmethod
    def create_audit_manager(self) -> TableDeliveryAuditManager:
        """Create the adapter that loads plans and persists bounded audit evidence."""

        raise NotImplementedError

    @abstractmethod
    def create_table_executor(self) -> TableDeliveryExecutor:
        """Create the executor that constructs and runs one table's runtime components."""

        raise NotImplementedError

    def run(
        self,
        environment: Mapping[str, str] = os.environ,
    ) -> ExecutionGroupOutcome:
        """Process every table delivery frozen for the scheduled parent pipeline run."""

        pipeline_run_id = _required(environment, "PIPELINE_RUN_ID")
        return ExecutionGroupRunner(
            self.create_audit_manager(),
            self.create_table_executor(),
        ).run(pipeline_run_id)


def _required(environment: Mapping[str, str], variable_name: str) -> str:
    value = environment.get(variable_name)
    if value is None or not value.strip():
        raise ValueError(f"{variable_name} must be set")
    return value


__all__ = ["ExecutionGroupJob"]