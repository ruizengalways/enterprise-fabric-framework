# Repository guidance for coding agents

Read [`docs/README.md`](docs/README.md) before changing architecture or runtime behavior. It lists
the canonical document for every topic and the required reading order.

Rules:

1. Read `docs/USAGE_MODEL.md` before adding a capability and identify the use case it serves.
2. Treat `docs/architecture/*.md` as the current specification.
3. Record unresolved contract choices only in `docs/decisions/OPEN.md`.
4. Update one canonical source instead of copying the same rule into multiple files.
5. Keep production business-data processing in Spark/Delta. Do not introduce a Python/pandas/list
   business engine or test oracle.
6. Domain-specific Gold logic and Fabric workspace items do not belong in this package.
7. Preserve the public runtime path in tests and certification.

When an open decision is resolved, update its canonical architecture section and remove it from
`OPEN.md`.
