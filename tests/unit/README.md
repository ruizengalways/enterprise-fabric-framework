# Python unit tests

Fast tests for configuration, contracts, orchestration, control-plane state, evidence and
platform protocol handling. Business load results do not have a Python in-memory oracle.

Unit tests must not recreate APPEND, REPLACE, UPSERT, SCD1, SCD2, SNAPSHOT_DIFF, CDC or projection
semantics using lists, dictionaries, pandas or another local engine.
