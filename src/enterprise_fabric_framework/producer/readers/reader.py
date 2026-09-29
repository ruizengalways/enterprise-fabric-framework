"""Abstract source-reader contract for Source-to-Bronze pipeline capabilities."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol, Self, TypeAlias

DataFrame = Any


class SourceReadable(Protocol):
    """Structural contract required by source readers, independent of connector inheritance."""

    def read(self, table_name: str) -> DataFrame:
        """Read all rows from one configured physical source object."""

        ...


class SourceCaptureMode(StrEnum):
    """Supported source-read modes selected by the frozen producer plan."""

    FULL = "FULL"
    WATERMARK = "WATERMARK"


@dataclass(frozen=True, slots=True)
class SourceReadPlan:
    """Source-specific portion of the configuration frozen before a read starts.

    Args:
        source_object: Connector-specific source name, such as ``crm.customer``.
        capture_mode: Whether this run reads a full snapshot or a bounded watermark interval.
        watermark_column: Changed-row column required for a watermark read.
    """

    source_object: str
    capture_mode: SourceCaptureMode
    watermark_column: str | None = None

    def __post_init__(self) -> None:
        if not self.source_object.strip():
            raise ValueError("source_object must not be empty")
        if self.capture_mode is SourceCaptureMode.WATERMARK and not self.watermark_column:
            raise ValueError("watermark_column is required for a WATERMARK read")


@dataclass(frozen=True, slots=True)
class SnapshotReadBoundary:
    """Source version selected for a complete snapshot read.

    ``source_version`` is optional because some sources cannot expose a stable snapshot version.
    When available, the pipeline manager proves and supplies it before the reader starts.
    """

    source_version: str | int | None = None


@dataclass(frozen=True, slots=True)
class WatermarkReadBoundary:
    """Inclusive lower and exclusive upper positions for a watermark read."""

    lower_bound: object
    upper_bound: object

    def __post_init__(self) -> None:
        if self.lower_bound is None or self.upper_bound is None:
            raise ValueError("watermark boundaries must not be None")


SourceReadBoundary: TypeAlias = SnapshotReadBoundary | WatermarkReadBoundary


@dataclass(frozen=True, slots=True)
class SourceReadEvidence:
    """Facts observed or supplied for one completed source read.

    ``source_row_count`` remains ``None`` unless a reader obtains it without changing the
    source-read contract. Readers must not trigger an additional Spark action solely to populate
    this field unless their capability explicitly requires that cost.
    """

    source_object: str
    capture_mode: SourceCaptureMode
    boundary: SourceReadBoundary
    source_row_count: int | None = None

    def __post_init__(self) -> None:
        if self.source_row_count is not None and self.source_row_count < 0:
            raise ValueError("source_row_count must be non-negative")


@dataclass(frozen=True, slots=True)
class SourceReadRequest:
    """Input assembled by the pipeline manager for one frozen source read.

    Args:
        plan: Source configuration selected before the run starts.
        boundary: Boundary proven by the pipeline manager before invoking the reader.

    Example:
        >>> plan = SourceReadPlan(
        ...     source_object="crm.customer",
        ...     capture_mode=SourceCaptureMode.WATERMARK,
        ...     watermark_column="modified_at",
        ... )
        >>> request = SourceReadRequest.for_watermark(
        ...     plan=plan,
        ...     lower_bound="2026-09-24T00:00:00Z",
        ...     upper_bound="2026-09-25T00:00:00Z",
        ... )
    """

    plan: SourceReadPlan
    boundary: SourceReadBoundary

    def __post_init__(self) -> None:
        if self.plan.capture_mode is SourceCaptureMode.FULL:
            if not isinstance(self.boundary, SnapshotReadBoundary):
                raise TypeError("a FULL read requires a SnapshotReadBoundary")
        elif self.plan.capture_mode is SourceCaptureMode.WATERMARK:
            if not isinstance(self.boundary, WatermarkReadBoundary):
                raise TypeError("a WATERMARK read requires a WatermarkReadBoundary")
        else:
            raise ValueError(f"unsupported capture mode: {self.plan.capture_mode!r}")

    @classmethod
    def for_snapshot(
        cls,
        plan: SourceReadPlan,
        source_version: str | int | None = None,
    ) -> Self:
        """Create a request for a complete source snapshot."""

        return cls(plan=plan, boundary=SnapshotReadBoundary(source_version=source_version))

    @classmethod
    def for_watermark(
        cls,
        plan: SourceReadPlan,
        lower_bound: object,
        upper_bound: object,
    ) -> Self:
        """Create a request for one inclusive/exclusive watermark interval."""

        return cls(
            plan=plan,
            boundary=WatermarkReadBoundary(
                lower_bound=lower_bound,
                upper_bound=upper_bound,
            ),
        )


@dataclass(slots=True)
class SourceReadResult:
    """Source rows and bounded evidence produced by a reader.

    Args:
        dataframe: Spark DataFrame containing the rows selected from the source.
        evidence: Bounded facts about the read, such as source counts and the proven boundary.

    Example:
        >>> result = SourceReadResult(dataframe=source_dataframe, evidence=read_evidence)
        >>> result.dataframe
        DataFrame[customer_id: bigint, modified_at: timestamp]
    """

    dataframe: DataFrame
    evidence: SourceReadEvidence


class SourceReader(ABC):
    """Base class for a customized Source-to-Bronze reader.

    Subclass this type when a producer pipeline needs source-specific capture logic and bounded
    evidence. Pass a concrete connector, such as ``DeltaSharingConnector``, to the reader. This
    base owns snapshot and watermark capture semantics; the connector owns its physical transport
    setup and primitive table read. A pipeline manager builds ``SourceReadRequest`` after it has
    frozen the plan, acquired any required lease, and proven the source boundary. The reader does
    not own lifecycle logging or cursor advancement.

    Example:
        >>> class CustomerReader(SourceReader):
        ...     def read(self, request):
        ...         dataframe = self.read_watermark(
        ...             table_name="crm.customer",
        ...             watermark_column="modified_at",
        ...             lower_bound=request.boundary.lower_bound,
        ...             upper_bound=request.boundary.upper_bound,
        ...         )
        ...         return self.result_for(request, dataframe)
        >>> result = CustomerReader(delta_sharing_connector).read(request)
        >>> result.dataframe.count()
        250
    """

    def __init__(self, connector: SourceReadable) -> None:
        """Create a reader with one configured physical source connector."""

        self._connector = connector

    def snapshot(self, table_name: str) -> DataFrame:
        """Read a complete snapshot from one configured source object."""

        return self._connector.read(table_name)

    def result_for(
        self,
        request: SourceReadRequest,
        dataframe: DataFrame,
        *,
        source_row_count: int | None = None,
    ) -> SourceReadResult:
        """Build the standard result for rows selected by ``request``.

        Specialized readers can pass a count obtained from a source-native statistic or override
        this method to add capability-specific evidence.
        """

        return SourceReadResult(
            dataframe=dataframe,
            evidence=SourceReadEvidence(
                source_object=request.plan.source_object,
                capture_mode=request.plan.capture_mode,
                boundary=request.boundary,
                source_row_count=source_row_count,
            ),
        )

    def read_from_request(self, request: SourceReadRequest) -> SourceReadResult:
        """Execute the generic read behavior selected by a validated request.

        Subclasses normally implement ``read`` by calling this method. A snapshot-only reader
        inherits the default ``supports_watermark = False`` and rejects incompatible requests.
        """

        if request.plan.capture_mode is SourceCaptureMode.FULL:
            return self.result_for(
                request,
                self.snapshot(request.plan.source_object),
            )
        if request.plan.capture_mode is SourceCaptureMode.WATERMARK:
            if not self.supports_watermark:
                raise NotImplementedError(
                    f"{type(self).__name__} does not support WATERMARK reads"
                )
            boundary = request.boundary
            if not isinstance(boundary, WatermarkReadBoundary):
                raise TypeError("a WATERMARK read requires a WatermarkReadBoundary")
            return self.result_for(
                request,
                self.read_watermark(
                    table_name=request.plan.source_object,
                    watermark_column=request.plan.watermark_column or "",
                    lower_bound=boundary.lower_bound,
                    upper_bound=boundary.upper_bound,
                ),
            )
        raise ValueError(f"unsupported capture mode: {request.plan.capture_mode!r}")

    supports_watermark = False

    def read_watermark(
        self,
        table_name: str,
        watermark_column: str,
        lower_bound: object,
        upper_bound: object,
    ) -> DataFrame:
        """Read rows where ``lower_bound <= watermark_column < upper_bound``.

        Override this method when a reader needs a source-specific watermark implementation.
        """

        dataframe = self._connector.read(table_name)
        return dataframe.where(
            (dataframe[watermark_column] >= lower_bound)
            & (dataframe[watermark_column] < upper_bound)
        )

    @abstractmethod
    def read(self, request: SourceReadRequest) -> SourceReadResult:
        """Read source facts and return the bounded evidence for one frozen request.

        Args:
            request: Frozen source configuration and read boundary.

        Returns:
            Source rows as a Spark DataFrame and bounded evidence describing the read.
        """

        raise NotImplementedError


class SnapshotSourceReader(SourceReader):
    """Generic concrete reader for sources that support complete snapshots only."""

    def read(self, request: SourceReadRequest) -> SourceReadResult:
        """Read the requested source snapshot or reject an unsupported watermark request."""

        return self.read_from_request(request)


__all__ = [
    "SnapshotReadBoundary",
    "SnapshotSourceReader",
    "SourceCaptureMode",
    "SourceReadBoundary",
    "SourceReadEvidence",
    "SourceReadPlan",
    "SourceReadRequest",
    "SourceReadResult",
    "SourceReader",
    "WatermarkReadBoundary",
]
