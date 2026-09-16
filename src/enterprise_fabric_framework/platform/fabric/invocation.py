"""Fabric Pipeline, Notebook and Spark Job invocation adapter boundary.

The implementation will validate explicit bounded parameters and normalize them into the public
Spark run request. It must not query ambient Pipeline state, discover a latest Bronze capture or
create/update Fabric workspace items.
"""

__all__: tuple[str, ...] = ()
