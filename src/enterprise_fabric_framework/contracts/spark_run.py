"""Contracts for the public Spark dataset runtime.

The first implementation PR will define versioned request and bounded evidence models here.
Requests will carry a validated capture identity, relation references and source boundaries, never
business rows. Fabric-specific Pipeline/Notebook/Job parameters are normalized before this layer.
"""

__all__: tuple[str, ...] = ()
