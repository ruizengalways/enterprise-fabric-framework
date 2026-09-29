"""Common base class for physical framework connectors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum


class ConnectionHealthStatus(StrEnum):
    """The bounded outcome of a connector health check."""

    HEALTHY = "HEALTHY"
    UNHEALTHY = "UNHEALTHY"
    NOT_CONFIGURED = "NOT_CONFIGURED"


@dataclass(frozen=True, slots=True)
class ConnectionHealth:
    """Bounded evidence returned by a connector health check."""

    connector_type: str
    status: ConnectionHealthStatus
    resource_ref: str | None = None
    detail: str | None = None


class Connector(ABC):
    """Base class for configured physical transports.

    Every connector returns bounded health evidence. Connector-specific operations stay with their
    owning runtime: Spark readers own source reads, SQL connectors own database sessions, and API
    connectors own credential renewal.
    """

    @abstractmethod
    def health(self) -> ConnectionHealth:
        """Return bounded evidence that this connector can reach its configured destination."""

        raise NotImplementedError

    def close(self) -> None:
        """Release connector resources when a concrete implementation owns any."""

        return None


__all__ = ["ConnectionHealth", "ConnectionHealthStatus", "Connector"]