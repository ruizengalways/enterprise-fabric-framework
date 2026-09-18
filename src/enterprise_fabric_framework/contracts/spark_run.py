"""Contracts for the public Spark dataset runtime.

The first implementation PR will define versioned request and bounded evidence models here.
Requests will carry a frozen producer/consumer plan, relation and checkpoint references, and optional
approved replay selection, never business rows. Fabric-specific Pipeline/Notebook/Job parameters
are normalized before this layer; Spark owns streaming offsets and query state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

RunStatus = Literal["SUCCEEDED", "FAILED", "CANCELLED"]


@dataclass(frozen=True, slots=True)
class BronzeRunRequest:
    request_schema_version: int
    bronze_run_id: str
    execution_request_id: str | None = None


@dataclass(frozen=True, slots=True)
class SilverRunRequest:
    request_schema_version: int
    silver_run_id: str
    execution_request_id: str | None = None


SparkRunRequest = BronzeRunRequest | SilverRunRequest


@dataclass(frozen=True, slots=True)
class RunError:
    code: str
    message: str
    evidence_ref: str | None = None


@dataclass(frozen=True, slots=True)
class RunResult:
    run_id: str
    status: RunStatus
    evidence_refs: tuple[str, ...] = ()
    error: RunError | None = None


__all__ = [
    "BronzeRunRequest",
    "SilverRunRequest",
    "SparkRunRequest",
    "RunError",
    "RunResult",
]
