"""Versioned bounded inputs and evidence for append-only source-to-Bronze deliveries.

The concrete model will contain producer/delivery identities, snapshot completeness, source
boundaries and relation/file references, never business rows. Silver streams consume configured
Bronze relations through independent checkpoints rather than mandatory per-capture run claims.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SourceReadBoundary:
    source_boundary_ref: str | None = None
    source_object_ref: str | None = None
    selection_ref: str | None = None
    checkpoint_ref: str | None = None


@dataclass(frozen=True, slots=True)
class SourceReadEvidence:
    delivery_id: str
    source_boundary_ref: str | None = None
    selected_rows: int | None = None
    evidence_ref: str | None = None


__all__ = ["SourceReadBoundary", "SourceReadEvidence"]
