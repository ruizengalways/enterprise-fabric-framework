# Environment bindings

`dev.yaml`, `uat.yaml` and `prod.yaml` contain only physical bindings and operational settings.
They are deliberately structurally identical so promotion changes configuration, not code.

Secrets must be referenced by logical secret/connection names and must never be committed here.
The files are design placeholders until the configuration contract is approved.

Bindings resolve typed source, Bronze and Silver `RelationRef` values to engine-specific physical
resources. Resolution evidence records stable Fabric item identifiers as well as readable names.
Environment bindings do not contain Gold topology or redefine dataset semantics.
