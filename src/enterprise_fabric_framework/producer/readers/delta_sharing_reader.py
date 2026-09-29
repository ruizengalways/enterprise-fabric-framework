"""Delta Sharing source-reader implementation."""

from __future__ import annotations

from enterprise_fabric_framework.connector.delta_sharing_connector import DeltaSharingConnector

from .reader import SourceReadRequest, SourceReadResult, SourceReader



class DeltaSharingReader(SourceReader):
    """Read Delta Sharing tables using snapshot or watermark request semantics.

    Args:
        connector: Configured Delta Sharing transport for this reader.

    Example:
        >>> reader = DeltaSharingReader(delta_sharing_connector)
        >>> result = reader.read(source_read_request)
        >>> result.evidence.capture_mode
        <SourceCaptureMode.WATERMARK: 'WATERMARK'>
    """

    supports_watermark = True

    def __init__(self, connector: DeltaSharingConnector) -> None:
        """Create a reader over one configured Delta Sharing connector."""

        super().__init__(connector)

    def read(self, request: SourceReadRequest) -> SourceReadResult:
        """Read the snapshot or watermark interval selected by ``request``."""

        return self.read_from_request(request)


__all__ = ["DeltaSharingReader"]
