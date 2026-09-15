# Real Fabric integration tests

Tests for OneLake, Lakehouse catalogs, managed identity, Spark Job Definitions, Pipelines,
installed candidate wheels, CDF retention and provider-specific concurrency behavior.

These tests execute the same public Spark runtime used by local Spark CI and production. They do
not call certification-specific business algorithms or compare results with a Python engine.
