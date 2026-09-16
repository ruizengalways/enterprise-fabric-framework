"""Registered Spark/Delta APPEND, REPLACE, UPSERT, SCD1, SCD2 and SNAPSHOT_DIFF executors.

DatasetRunner resolves these internal strategies from a frozen typed dataset run. Pipelines and
domain code do not instantiate them directly.
"""

__all__: tuple[str, ...] = ()
