# Test suites

The complete testing architecture and target directory layout are defined in
[`docs/architecture/TESTING_STRATEGY.md`](../docs/architecture/TESTING_STRATEGY.md).

Tests are separated by the runtime they prove. A test may not claim production business-semantic
coverage unless it executes the public Spark runtime against real Spark and Delta.

The repository does not contain pure-Python in-memory implementations of business strategies and
does not use them as test oracles. CI validates expected relations, Delta history and invariants
through production Spark code. Local Spark success remains distinct from real Fabric
certification; required Fabric behavior is proven in `fabric/` and `uat/` with the exact candidate
wheel and the same public runtime entry.
