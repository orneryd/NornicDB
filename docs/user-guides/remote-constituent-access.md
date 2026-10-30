# Remote constituent access modes

A composite constituent's configured `AccessMode` limits execution independently
of the current user's grants and the remote account's permissions. The supported
values are `read`, `write` and `read_write`. Mutation requires a writable mode;
`write` retains the existing ability to read through the alias as well.

For a `read` remote constituent, ordinary reads and known read-only procedures
remain available, for example:

```cypher
USE catalog.reporting MATCH (n) RETURN count(n)
```

```cypher
USE catalog.reporting CALL db.labels() YIELD label RETURN label
```

Writes, schema and administration operations are rejected before forwarding.
A remote procedure absent from the coordinator's registry, or registered as
executing nested statements, is also rejected through a read-only alias: its
remote effects cannot be established by local nested-statement admission.
Use a writable alias when the operation is intended to have those capabilities.
This restriction also applies when local authentication is disabled.

The same policy applies to direct alias selection and resolved dynamic graph
selection in a composite session. Direct remote HTTP explicit transaction
startup remains unsupported; implicit execution and supported Fabric transaction
paths retain their existing transaction ownership.

A policy rejection uses `Neo.ClientError.Security.Forbidden`, with a read-only
constituent message. In Go, `PermissionDeniedError.Permission == "read-only"`
identifies this configured restriction rather than a missing principal grant.
