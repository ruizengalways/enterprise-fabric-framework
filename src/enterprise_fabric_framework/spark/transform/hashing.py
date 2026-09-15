"""Schema-aware Spark Column expressions for distributed business-row hashing.

Canonical null, timestamp, decimal, binary and nested-value behavior must be specified before
implementation. This module must never collect rows to calculate a hash in Python.
"""

__all__: tuple[str, ...] = ()
