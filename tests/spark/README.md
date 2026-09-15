# Local Spark and Delta tests

These tests execute the same public Spark runtime entry used by Fabric. They use fixed relational
fixtures, Delta history and invariants instead of a second Python implementation.

A local PASS proves the Spark/Delta code path under the pinned local compatibility runtime. It
does not replace real Fabric UAT and must not be reported as Fabric certification.
