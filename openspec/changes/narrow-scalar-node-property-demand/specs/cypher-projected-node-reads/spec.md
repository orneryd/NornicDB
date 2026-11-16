## ADDED Requirements

### Requirement: A label scan reads only the property bodies its rows use

A label scan for a labelled `MATCH` SHALL decode a stored property body only
when a row of the statement reads it, and SHALL report the demand it used. The
identity and labels of a matched node SHALL remain available without decoding a
property body, and a mutation, a native full-image consumer and a returned node
SHALL keep the complete property body.

#### Scenario: Identity, labels and existence read the header

- **WHEN** `MATCH (n:EIBig) RETURN id(n)`, `elementId(n)`, `labels(n)`,
  `n IS NULL`, `n IS NOT NULL` or `MATCH (n:EIBig) WHERE n IS NOT NULL RETURN 1`
  executes over a node with an unrelated 128 KiB property
- **THEN** the label scan names no property, the delivered image carries the
  node's identity and labels and no decoded body, and the answer equals the
  whole-body answer

#### Scenario: A list's size reads only the elements' headers

- **WHEN** `MATCH (n:EIBig) RETURN size([n])` or `size(collect(n))` executes
- **THEN** the label scan names no property and the statement returns 1

#### Scenario: A WITH pass-through keeps the demand of the clauses after it

- **WHEN** `MATCH (n:EIBig) WITH n RETURN 1` executes
- **THEN** the label scan names no property

#### Scenario: A named property reads that property alone

- **WHEN** `MATCH (n:EIBig) RETURN n.small`, `n['small']`, `(n).small` or
  `head([n]).small` executes
- **THEN** the label scan names only `small` and returns the property's value

#### Scenario: A WITH rename is followed by name

- **WHEN** `MATCH (p:P) WITH p AS q RETURN q.b AS b ORDER BY b` executes
- **THEN** the label scan names only `b` and the rows are the three stored
  `b` values in order

#### Scenario: A WITH that stops carrying the variable ends its demand soundly

- **WHEN** `MATCH (p:P) WITH collect(p) AS ps RETURN size(ps)` executes
- **THEN** the scan still reads the whole body, because the aggregate output is
  not the variable and the collection reads each node's value

### Requirement: A node value that escapes keeps every property body

A node read as a value by an operation the statement does not narrow SHALL keep
the complete property body, including a returned node, `properties()`, a list or
map escape, a native callback and an unknown call.

#### Scenario: A returned node keeps the whole body

- **WHEN** `MATCH (n:EIBig) RETURN n` executes
- **THEN** the row's node carries every stored property and the read asks
  storage for the query-row metadata hydration of a whole-node read

#### Scenario: A native consumer keeps the whole body and the vectors

- **WHEN** `MATCH (n:EIBig) RETURN image.scalarNodeProbe(n)` executes
- **THEN** the read demands every property body and the delivered image keeps
  the stored vectors, and a callback's edits never reach the stored node

#### Scenario: A computed subscript and an unknown operation keep the whole body

- **WHEN** `MATCH (n:EIBig) RETURN n['a' + 'b']`, `n[$key]`, `[n]`, `{node: n}`
  or `p.a + p` executes
- **THEN** the label scan demands every property body
