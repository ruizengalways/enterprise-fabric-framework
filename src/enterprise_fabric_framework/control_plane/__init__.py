"""Control-plane port, default SQL adapter, query leases, audit and bounded run state.

Deployments MAY replace the SQL adapter with a company/domain-owned implementation. Spark
Structured Streaming owns authoritative query offsets and checkpoint contents.
"""

from .ports import ControlPlanePort
from .sql_adapter import SqlControlPlaneAdapter

__all__ = ["ControlPlanePort", "SqlControlPlaneAdapter"]
