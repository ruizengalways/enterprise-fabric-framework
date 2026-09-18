"""Spark/Delta derived projections such as authoritative-history current state."""

from .current import ProjectionResult, project_current_state

__all__ = ["ProjectionResult", "project_current_state"]
