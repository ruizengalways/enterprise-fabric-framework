"""Typed frozen-SQL policy mapping, dataset semantics and capability declarations.

SQL access belongs to ``control_plane`` and Fabric physical binding belongs to
``platform.fabric``; this package does not load YAML or connect to the database.
"""

from .mapping import (
    map_bronze_policy,
    map_bronze_to_silver_config,
    map_rule,
    map_silver_policy,
    map_source_to_bronze_config,
)

__all__ = [
    "map_bronze_policy",
    "map_bronze_to_silver_config",
    "map_rule",
    "map_silver_policy",
    "map_source_to_bronze_config",
]
