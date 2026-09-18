"""Normalize explicit Microsoft Fabric invocation context for the public runtime.

Fabric workspace item provisioning and deployment are deliberately outside this package.
"""

from .invocation import FabricInvocation, normalize_invocation, run_fabric_invocation, to_spark_request

__all__ = [
    "FabricInvocation",
    "normalize_invocation",
    "run_fabric_invocation",
    "to_spark_request",
]
