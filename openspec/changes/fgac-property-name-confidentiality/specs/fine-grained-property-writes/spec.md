## ADDED Requirements

### Requirement: Authorize property names on restricted writes

Under FGAC, a principal without the database's property-name creation privilege SHALL have property-read and property-write access on the target to explicitly set or remove a named node or relationship property. Setting a value SHALL also require a registered property name. The denial SHALL use the same forbidden error for an unreadable registered name and a new name. A whole-map replacement by such a principal SHALL preserve omitted unreadable properties.

#### Scenario: Registered unreadable name and unregistered name

- **WHEN** a principal with property-write access and without property-name creation privilege sets a value using an unreadable registered name or an unregistered name
- **THEN** both writes fail with the same forbidden error and the graph retains its previous state

#### Scenario: Omitted unreadable name in a replacement

- **WHEN** a principal without property-name creation privilege replaces an entity's properties with an authorized map
- **THEN** omitted readable properties are subject to the regular write checks, and omitted unreadable properties retain their values

#### Scenario: Writer with property-name creation privilege

- **WHEN** a principal with property-name creation and applicable property-write privileges writes an unreadable name or registers a new name
- **THEN** the write succeeds and read permissions continue to govern subsequent reads
