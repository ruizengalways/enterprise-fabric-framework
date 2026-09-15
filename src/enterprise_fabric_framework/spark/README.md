# Spark runtime

This package owns all production business-data computation. Its public entry point will be
`SparkDatasetRuntime.run(request) -> SparkRunEvidence`.

Subpackages separate capture, transformation, load, reconciliation and projection for code
ownership. They do not create alternative end-to-end entry points.
