# Configuration

Configuration is split into two independently versioned concerns:

- `datasets/` contains environment-independent dataset semantics.
- `environments/` binds logical references to Dev, UAT or Prod Fabric resources.

Dataset files must not contain workspace IDs, secrets, environment-specific table names or
environment-specific code switches. Environment files must not redefine business rules.

Neither directory may contain one-time commands such as `run_mode: REBUILD`, pending approvals,
request status or consumed markers. Those are bounded control-plane `ExecutionRequest` records
created through the governed operational workflow in ADR 0019.

The concrete configuration schema is intentionally not implemented in this scaffold. It will
be introduced with the first contract PR after the architecture decisions are accepted.
