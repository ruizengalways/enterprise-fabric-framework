"""Table-isolated execution orchestration over frozen control-plane deliveries."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum

from enterprise_fabric_framework.quality import QualityRunResult
from enterprise_fabric_framework.reconciliation import ReconciliationResult


class TableDeliveryStatus(StrEnum):
    """Terminal status for one independently planned table delivery."""

    SUCCEEDED = "SUCCEEDED"
    SUCCEEDED_WITH_WARNINGS = "SUCCEEDED_WITH_WARNINGS"
    FAILED = "FAILED"


class ExecutionGroupStatus(StrEnum):
    """Aggregate terminal status for a group of independent table deliveries."""

    SUCCEEDED = "SUCCEEDED"
    SUCCEEDED_WITH_WARNINGS = "SUCCEEDED_WITH_WARNINGS"
    PARTIAL_FAILURE = "PARTIAL_FAILURE"


@dataclass(frozen=True, slots=True)
class TableDelivery:
    """One frozen table delivery owned by a parent pipeline execution."""

    pipeline_run_id: str
    bronze_run_id: str
    source_to_bronze_config_id: str
    source_object: str
    ingestion_method_ref: str = "controlled-delta-sharing@1"
    data_quality_rule_refs: tuple[str, ...] = ()
    reconciliation_rule_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for field_name in (
            "pipeline_run_id",
            "bronze_run_id",
            "source_to_bronze_config_id",
            "source_object",
            "ingestion_method_ref",
        ):
            if not getattr(self, field_name).strip():
                raise ValueError(f"{field_name} must not be empty")
        for field_name in ("data_quality_rule_refs", "reconciliation_rule_refs"):
            rule_refs = getattr(self, field_name)
            if any(not rule_ref.strip() for rule_ref in rule_refs):
                raise ValueError(f"{field_name} must not contain empty rule references")


@dataclass(frozen=True, slots=True)
class TableDeliveryEvidence:
    """Bounded evidence returned by a per-table Spark/Delta execution."""

    quality_result: QualityRunResult | None = None
    reconciliation_result: ReconciliationResult | None = None
    evidence_ref: str | None = None


@dataclass(frozen=True, slots=True)
class TableDeliveryOutcome:
    """Terminal audit result for one table delivery; never contains business rows."""

    delivery: TableDelivery
    status: TableDeliveryStatus
    evidence: TableDeliveryEvidence | None = None
    error_code: str | None = None


@dataclass(frozen=True, slots=True)
class ExecutionGroupOutcome:
    """Aggregate result of processing every planned table in one pipeline invocation."""

    pipeline_run_id: str
    status: ExecutionGroupStatus
    table_outcomes: tuple[TableDeliveryOutcome, ...]


class TableDeliveryExecutor(ABC):
    """Customizable implementation that executes one frozen table delivery.

    Implementations perform all Spark/Delta work for one table and return only bounded DQ and
    reconciliation results. Expected critical rule failures must be returned in the evidence;
    unexpected runtime failures may raise and are isolated to this table by ``ExecutionGroupRunner``.
    """

    @abstractmethod
    def execute(self, delivery: TableDelivery) -> TableDeliveryEvidence:
        """Execute one table delivery using its frozen run identity."""

        raise NotImplementedError


class TableIngestionMethod(ABC):
    """Customizable Spark/Delta implementation for one frozen ingestion method."""

    @abstractmethod
    def execute(
        self,
        delivery: TableDelivery,
        *,
        quality_evaluator: object | None,
        reconciliation_evaluator: object | None,
    ) -> TableDeliveryEvidence:
        """Run one table using components constructed for its frozen delivery plan."""

        raise NotImplementedError


class TableDeliveryRuntimeFactory(ABC):
    """Customizable resolver for one delivery's ingestion and rule capabilities.

    Implementations resolve the references frozen on ``TableDelivery`` against the deployment's
    capability registry. The methods are invoked once per delivery, so table-specific reader,
    writer, DQ, and reconciliation configuration cannot leak into a later table.
    """

    @abstractmethod
    def create_ingestion_method(self, delivery: TableDelivery) -> TableIngestionMethod:
        """Create the ingestion method selected by ``delivery.ingestion_method_ref``."""

        raise NotImplementedError

    @abstractmethod
    def create_data_quality_evaluator(self, delivery: TableDelivery) -> object | None:
        """Construct the DQ evaluator from this delivery's frozen rule references."""

        raise NotImplementedError

    @abstractmethod
    def create_reconciliation_evaluator(self, delivery: TableDelivery) -> object | None:
        """Construct the reconciliation evaluator from this delivery's frozen rule references."""

        raise NotImplementedError


class RuntimeConfiguredTableDeliveryExecutor(TableDeliveryExecutor):
    """Construct table-specific runtime components immediately before every delivery."""

    def __init__(self, factory: TableDeliveryRuntimeFactory) -> None:
        self._factory = factory

    def execute(self, delivery: TableDelivery) -> TableDeliveryEvidence:
        """Resolve and run the method, DQ, and reconciliation components for one table."""

        ingestion_method = self._factory.create_ingestion_method(delivery)
        quality_evaluator = self._factory.create_data_quality_evaluator(delivery)
        reconciliation_evaluator = self._factory.create_reconciliation_evaluator(delivery)
        return ingestion_method.execute(
            delivery,
            quality_evaluator=quality_evaluator,
            reconciliation_evaluator=reconciliation_evaluator,
        )


class TableDeliveryAuditManager(ABC):
    """Customizable control-plane port for planned deliveries and bounded audit evidence."""

    @abstractmethod
    def load_table_deliveries(self, pipeline_run_id: str) -> Sequence[TableDelivery]:
        """Load the already planned frozen table deliveries for one parent run."""

        raise NotImplementedError

    @abstractmethod
    def record_quality_result(
        self,
        delivery: TableDelivery,
        result: QualityRunResult,
    ) -> None:
        """Persist one table's bounded DQ result through the control plane."""

        raise NotImplementedError

    @abstractmethod
    def record_reconciliation_result(
        self,
        delivery: TableDelivery,
        result: ReconciliationResult,
    ) -> None:
        """Persist one table's bounded reconciliation result through the control plane."""

        raise NotImplementedError

    @abstractmethod
    def record_table_outcome(self, outcome: TableDeliveryOutcome) -> None:
        """Persist the terminal status for exactly one table delivery."""

        raise NotImplementedError


class ExecutionGroupRunner:
    """Sequentially execute every frozen table without one failure stopping other tables.

    Use this runner inside one Spark Job Definition or a thin Fabric Notebook launcher. Fabric
    Pipeline ``ForEach`` may instead invoke one job per delivery for bounded parallelism; both
    arrangements use the same delivery, executor, and audit-manager contracts.
    """

    def __init__(
        self,
        audit_manager: TableDeliveryAuditManager,
        executor: TableDeliveryExecutor,
    ) -> None:
        self._audit_manager = audit_manager
        self._executor = executor

    def run(self, pipeline_run_id: str) -> ExecutionGroupOutcome:
        """Run all planned tables and return an aggregate status after every attempt completes."""

        if not pipeline_run_id.strip():
            raise ValueError("pipeline_run_id must not be empty")
        outcomes: list[TableDeliveryOutcome] = []
        for delivery in self._audit_manager.load_table_deliveries(pipeline_run_id):
            outcomes.append(self._run_table_delivery(delivery))
        return ExecutionGroupOutcome(
            pipeline_run_id=pipeline_run_id,
            status=_execution_group_status(outcomes),
            table_outcomes=tuple(outcomes),
        )

    def _run_table_delivery(self, delivery: TableDelivery) -> TableDeliveryOutcome:
        try:
            evidence = self._executor.execute(delivery)
            if evidence.quality_result is not None:
                self._audit_manager.record_quality_result(delivery, evidence.quality_result)
            if evidence.reconciliation_result is not None:
                self._audit_manager.record_reconciliation_result(
                    delivery,
                    evidence.reconciliation_result,
                )
            outcome = TableDeliveryOutcome(
                delivery=delivery,
                status=_table_delivery_status(evidence),
                evidence=evidence,
            )
        except Exception as error:
            outcome = TableDeliveryOutcome(
                delivery=delivery,
                status=TableDeliveryStatus.FAILED,
                error_code=type(error).__name__,
            )
        self._audit_manager.record_table_outcome(outcome)
        return outcome


def _table_delivery_status(evidence: TableDeliveryEvidence) -> TableDeliveryStatus:
    if (
        evidence.quality_result is not None and evidence.quality_result.has_critical_failure
    ) or (
        evidence.reconciliation_result is not None
        and evidence.reconciliation_result.has_critical_failure
    ):
        return TableDeliveryStatus.FAILED
    if (evidence.quality_result is not None and evidence.quality_result.has_warnings) or (
        evidence.reconciliation_result is not None and evidence.reconciliation_result.has_warnings
    ):
        return TableDeliveryStatus.SUCCEEDED_WITH_WARNINGS
    return TableDeliveryStatus.SUCCEEDED


def _execution_group_status(
    outcomes: Sequence[TableDeliveryOutcome],
) -> ExecutionGroupStatus:
    if any(outcome.status is TableDeliveryStatus.FAILED for outcome in outcomes):
        return ExecutionGroupStatus.PARTIAL_FAILURE
    if any(outcome.status is TableDeliveryStatus.SUCCEEDED_WITH_WARNINGS for outcome in outcomes):
        return ExecutionGroupStatus.SUCCEEDED_WITH_WARNINGS
    return ExecutionGroupStatus.SUCCEEDED


__all__ = [
    "ExecutionGroupOutcome",
    "ExecutionGroupRunner",
    "ExecutionGroupStatus",
    "RuntimeConfiguredTableDeliveryExecutor",
    "TableDelivery",
    "TableDeliveryAuditManager",
    "TableDeliveryEvidence",
    "TableDeliveryExecutor",
    "TableDeliveryRuntimeFactory",
    "TableDeliveryOutcome",
    "TableDeliveryStatus",
    "TableIngestionMethod",
]