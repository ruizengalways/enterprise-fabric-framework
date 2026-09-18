"""Mapping boundary from deployed control-plane rows to frozen typed values.

This module owns shape conversion and validation hooks only. It does not open SQL connections,
execute Spark plans or infer missing policy values.
"""

from __future__ import annotations

from typing import Mapping

from enterprise_fabric_framework.contracts import (
    FrozenBronzePolicy,
    FrozenBronzeToSilverConfig,
    FrozenSilverPolicy,
    FrozenSourceToBronzeConfig,
    RuleSpec,
)


def map_source_to_bronze_config(
    row: Mapping[str, object],
) -> FrozenSourceToBronzeConfig:
    """Map one frozen Source-to-Bronze configuration row."""

    raise NotImplementedError


def map_bronze_to_silver_config(
    row: Mapping[str, object],
) -> FrozenBronzeToSilverConfig:
    """Map one frozen Bronze-to-Silver configuration row."""

    raise NotImplementedError


def map_bronze_policy(row: Mapping[str, object]) -> FrozenBronzePolicy:
    """Map one frozen Bronze policy row."""

    raise NotImplementedError


def map_silver_policy(row: Mapping[str, object]) -> FrozenSilverPolicy:
    """Map one frozen Silver policy row."""

    raise NotImplementedError


def map_rule(row: Mapping[str, object]) -> RuleSpec:
    """Map one frozen quality or reconciliation rule row."""

    raise NotImplementedError


__all__ = [
    "map_bronze_policy",
    "map_bronze_to_silver_config",
    "map_rule",
    "map_silver_policy",
    "map_source_to_bronze_config",
]
