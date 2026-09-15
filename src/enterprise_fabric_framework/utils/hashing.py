"""Canonical hashes for small configuration, contract and evidence objects.

This module must not hash distributed business rows. Spark row hashing belongs in
``spark.transform.hashing`` so data stays in Spark and schema semantics remain explicit.
"""

__all__: tuple[str, ...] = ()
