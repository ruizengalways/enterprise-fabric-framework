"""Composable Source-to-Bronze lifecycle and its customization boundaries."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from enterprise_fabric_framework.producer.readers.reader import (
        SourceReadRequest,
        SourceReadResult,
        SourceReader,
    )
    from enterprise_fabric_framework.producer.writers.base import (
        BronzeWriteResult,
        BronzeWriter,
    )


class DataQualityEvaluator(Protocol):
    """Evaluates a producer's declared quality contract without publishing data."""

    def evaluate(self, read_result: SourceReadResult) -> SourceReadResult:
        """Return the source data that is eligible for Bronze publication."""

        ...


class BronzeReconciler(Protocol):
    """Validates a completed Bronze publication against source evidence."""

    def reconcile(
        self,
        read_result: SourceReadResult,
        write_result: BronzeWriteResult,
    ) -> None:
        """Raise or record failure when source-to-Bronze reconciliation does not pass."""

        ...


@dataclass(frozen=True, slots=True)
class SourceToBronzeComponents:
    """Versioned capabilities selected and frozen by a producer plan."""

    reader: SourceReader
    writer: BronzeWriter
    quality_evaluator: DataQualityEvaluator | None = None
    reconciler: BronzeReconciler | None = None


@dataclass(frozen=True, slots=True)
class SourceToBronzeRunRequest:
    """Pipeline-owned orchestration input for one Source-to-Bronze run.

    Reader implementations receive only ``source_read_request``. The pipeline retains the
    run identifier because it owns publication, audit, and lifecycle orchestration.

    Args:
        bronze_run_id: Identity used by the pipeline to publish and audit this delivery.
        source_read_request: Source configuration and proven boundary required by the reader.
    """

    bronze_run_id: str
    source_read_request: SourceReadRequest


class SourceToBronzePipeline(ABC):
    """Runs one frozen Source-to-Bronze lifecycle.

    Replace a component for a localized policy change. Subclass this type only
    when the lifecycle order itself must differ from the standard sequence.
    """

    @abstractmethod
    def run(self, request: SourceToBronzeRunRequest) -> BronzeWriteResult:
        """Execute one producer lifecycle for a frozen source boundary."""

        raise NotImplementedError


class StandardSourceToBronzePipeline(SourceToBronzePipeline):
    """Default order: read, quality evaluation, Bronze publication, reconciliation."""

    def __init__(self, components: SourceToBronzeComponents) -> None:
        self._components = components

    def run(self, request: SourceToBronzeRunRequest) -> BronzeWriteResult:
        """Execute the standard producer lifecycle using injected capabilities."""

        from enterprise_fabric_framework.producer.writers.base import BronzeWriteRequest

        read_result = self._components.reader.read(request.source_read_request)
        quality_result = read_result
        if self._components.quality_evaluator is not None:
            quality_result = self._components.quality_evaluator.evaluate(read_result)
        write_result = self._components.writer.write(
            BronzeWriteRequest(
                bronze_run_id=request.bronze_run_id,
                dataframe=quality_result.dataframe,
                plan=request.source_read_request.plan,
                source_evidence=quality_result.evidence,
            )
        )
        if self._components.reconciler is not None:
            self._components.reconciler.reconcile(quality_result, write_result)
        return write_result


__all__ = [
    "BronzeReconciler",
    "DataQualityEvaluator",
    "SourceToBronzeComponents",
    "SourceToBronzePipeline",
    "SourceToBronzeRunRequest",
    "StandardSourceToBronzePipeline",
]