"""Capability selection and validation used while planning producer and consumer runs."""

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


class CapabilityRegistry(Protocol):
    """Registry of stable, versioned framework capabilities."""

    def describe(
        self,
        capability_ref: str,
        kind: CapabilityKind,
    ) -> CapabilityDescriptor:
        """Return one registered capability descriptor."""

        ...

    def resolve(self, capability_ref: str, kind: CapabilityKind) -> object:
        """Return one registered implementation."""

        ...


def require_capability(
    registry: CapabilityRegistry,
    capability_ref: str,
    kind: CapabilityKind,
) -> object:
    """Resolve a capability before a plan uses it."""

    raise NotImplementedError


__all__ = ["CapabilityDescriptor", "CapabilityKind", "CapabilityRegistry", "require_capability"]
