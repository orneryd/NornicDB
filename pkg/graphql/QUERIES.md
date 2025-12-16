# Basic Queries

```
# Get database stats
query Stats {
  stats {
    nodeCount
    relationshipCount
    embeddedNodeCount
    uptimeSeconds
    labels {
      label
      count
    }
  }
}

# List all labels in the database
query Labels {
  labels
}

# Get all relationship types
query RelationshipTypes {
  relationshipTypes
}

# Get a specific node by ID
query GetNode {
  node(id: "your-node-id-here") {
    id
    labels
    properties
    createdAt
    updatedAt
  }
}

# List nodes by label with pagination
query ListPersons {
  allNodes(labels: ["Person"], limit: 10, offset: 0) {
    id
    labels
    properties
  }
}
```

# Creating Data

```
# Create a person node
mutation CreatePerson {
  createNode(input: {
    labels: ["Person"]
    properties: {
      name: "Alice Smith"
      age: 30
      email: "alice@example.com"
    }
  }) {
    id
    labels
    properties
  }
}

# Create a company node
mutation CreateCompany {
  createNode(input: {
    labels: ["Company"]
    properties: {
      name: "TechCorp"
      industry: "Software"
      founded: 2020
    }
  }) {
    id
    labels
    properties
  }
}

# Create a relationship between nodes
mutation CreateWorksAt {
  createRelationship(input: {
    sourceId: "person-node-id"
    targetId: "company-node-id"
    type: "WORKS_AT"
    properties: {
      since: "2023-01-15"
      role: "Engineer"
    }
  }) {
    id
    type
    properties
    source { id labels properties }
    target { id labels properties }
  }
}
```

# Search Queries

```
# Full-text search
query SearchPeople {
  search(query: "software engineer", labels: ["Person"], limit: 10) {
    results {
      node {
        id
        labels
        properties
      }
      score
    }
    totalCount
    executionTimeMs
  }
}

# Find similar nodes (vector similarity)
query FindSimilar {
  similar(nodeId: "your-node-id", limit: 5) {
    node {
      id
      labels
      properties
    }
    similarity
  }
}
```

# Raw Cypher Queries

```
# Execute any Cypher query
query CypherQuery {
  cypher(input: {
    statement: "MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.name, c.name LIMIT 10"
  }) {
    columns
    rows
    rowCount
  }
}

# Cypher with parameters
query CypherWithParams {
  cypher(input: {
    statement: "MATCH (p:Person) WHERE p.age > $minAge RETURN p"
    parameters: { minAge: 25 }
  }) {
    columns
    rows
    rowCount
  }
}
```

# Update Ops

```
# Update a node's properties
mutation UpdatePerson {
  updateNode(id: "node-id-here", input: {
    properties: {
      age: 31
      title: "Senior Engineer"
    }
  }) {
    id
    properties
    updatedAt
  }
}

# Merge (upsert) - create or update
mutation MergePerson {
  mergeNode(input: {
    labels: ["Person"]
    matchProperties: { email: "alice@example.com" }
    setProperties: { lastLogin: "2024-12-16" }
  }) {
    id
    properties
  }
}
```

# Delete Ops

```
# Delete a node
mutation DeleteNode {
  deleteNode(id: "node-id-here")
}

# Delete a relationship
mutation DeleteRelationship {
  deleteRelationship(id: "relationship-id-here")
}
```

# Admin Ops

```
# Trigger embedding generation
mutation TriggerEmbedding {
  triggerEmbedding(regenerate: false) {
    pending
    embedded
    total
    workerRunning
  }
}

# Rebuild search index
mutation RebuildIndex {
  rebuildSearchIndex
}

# Get schema information
query Schema {
  schema {
    nodeLabels
    relationshipTypes
    nodePropertyKeys
  }
}
```

# Traversal

```
# Get node with relationships
query NodeWithRelationships {
  node(id: "person-node-id") {
    id
    labels
    properties
    outgoingRelationships(limit: 10) {
      id
      type
      properties
      target {
        id
        labels
        properties
      }
    }
    incomingRelationships(limit: 10) {
      id
      type
      source {
        id
        labels
        properties
      }
    }
  }
}
```
