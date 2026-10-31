# Schema creation APIs in Go

For v1.4.4, node and relationship full-text indexes use one schema mutation:

```go
err := schema.AddFulltextIndex("documents", []string{"Document"},
    []string{"title", "body"}, storage.ConstraintEntityNode)

err = schema.AddFulltextIndex("links", []string{"LINKS_TO"},
    []string{"note"}, storage.ConstraintEntityRelationship)
```

Migrate former three-argument `AddFulltextIndex` calls by supplying
`ConstraintEntityNode`. Replace `AddFulltextRelationshipIndex` calls with
`AddFulltextIndex` and supply `ConstraintEntityRelationship`.

The schema owns the supplied target and property slices. An existing name is
an idempotent no-op, including a collision with an index of the other entity
kind. Invalid entity kinds return `ErrInvalidData` without persisting or
publishing an index. Persistence failures retain the previous schema.

Cypher syntax, index definitions, Badger keys, stored payloads and the storage
version are unchanged; this Go API migration needs no database migration.

## Vector and property-type creation

Go callers now select vector entity scope directly with
`schema.AddVectorIndex(name, labelOrType, property, dimensions, similarity, entityType)`.
The node-only forwarding form and `AddVectorIndexForEntity` are retired; use
`storage.ConstraintEntityNode` or `storage.ConstraintEntityRelationship` explicitly.

Property-type creation uses
`schema.AddPropertyTypeConstraint(name, labelOrType, property, expectedType, opts)`,
where `opts` is `storage.PropertyTypeConstraintOptions`. Move an earlier optional
entity argument into `opts.EntityType`; set `opts.IfNotExists` when required.
The separate `AddPropertyTypeConstraintWithOptions` entry point is retired.
For example:

```go
err := schema.AddPropertyTypeConstraint("person_age", "Person", "age",
    storage.PropertyTypeInteger, storage.PropertyTypeConstraintOptions{
        EntityType: storage.ConstraintEntityNode,
        IfNotExists: true,
    })
```

These changes migrate Go call sites to one implementation for each operation.
Cypher and protocol syntax, existing persisted definitions and storage formats
are unchanged.
