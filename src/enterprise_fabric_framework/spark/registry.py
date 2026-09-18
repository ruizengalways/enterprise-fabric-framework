"""Spark capability registry and source of advertised production support.

The registry covers source readers, Bronze writers, transforms, load strategies, reconciliation
and projections. Capabilities use stable, versioned, business-neutral identities. None may be
advertised before a concrete executor and its required test evidence exist.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol

CapabilityKind = Literal[
    "SOURCE_READER",
    "BRONZE_WRITER",
    "TRANSFORM",
    "QUALITY_RULE",
    "LOAD_STRATEGY",
    "RECONCILIATION_RULE",
    "PROJECTION",
]


@dataclass(frozen=True, slots=True)
class CapabilityDescriptor:
    capability_ref: str
    kind: CapabilityKind
    request_schema_version: int
    evidence_schema_version: int
    certified: bool


class CapabilityRegistry(Protocol):
    """Registry seam for versioned, business-neutral Spark capabilities."""

    def describe(
        self,
        capability_ref: str,
        kind: CapabilityKind,
    ) -> CapabilityDescriptor:
        """Return the registered descriptor for one capability."""

        ...

    def resolve(self, capability_ref: str, kind: CapabilityKind) -> object:
        """Return the executable registered implementation."""

        ...


def require_capability(
    registry: CapabilityRegistry,
    capability_ref: str,
    kind: CapabilityKind,
) -> object:
    """Resolve a certified capability before a plan mutates data."""

    raise NotImplementedError


__all__ = ["CapabilityDescriptor", "CapabilityKind", "CapabilityRegistry", "require_capability"]
