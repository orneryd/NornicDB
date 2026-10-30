---
name: Performance Issue
about: Report a performance problem or regression
title: '[PERF] '
labels: performance
assignees: ''
---

## Performance Issue Description
Describe the performance problem you're experiencing.

## Environment
- **NornicDB Version**: [e.g., v1.0.0]
- **OS**: [e.g., macOS 14.5, Ubuntu 22.04]
- **Architecture**: [e.g., ARM64, AMD64]
- **CPU**: [e.g., Apple M3, Intel Xeon, AMD Ryzen]
- **RAM**: [e.g., 16GB, 32GB]
- **GPU** (if applicable): [e.g., Apple M3, NVIDIA RTX 4090]
- **Docker Image**: [e.g., arm64-metal-bge:latest, or "built from source"]

## Dataset Size
- **Nodes**: [e.g., 100K]
- **Relationships**: [e.g., 500K]
- **Properties**: [average per node/relationship]
- **Embeddings**: [if using vector features]

## Query
```cypher
// Your query here
```

## Performance Metrics
**Current Performance:**
- Query time: [e.g., 5.2 seconds]
- Memory usage: [e.g., 2GB]
- CPU usage: [e.g., 85%]

**Expected Performance:**
- Query time: [e.g., < 1 second]
- Memory usage: [e.g., < 1GB]

## Comparison
- [ ] This query performs better in Neo4j (provide Neo4j timing)
- [ ] This is slower than a previous NornicDB version (specify version)
- [ ] This is unexpectedly slow for the dataset size
- [ ] Other: ___

## Reproduction Steps
1. Create dataset with [description]
2. Run query [query]
3. Measure performance with [method]

## Additional Context
- Are there any relevant indexes?
- Have you tried query optimization?
- Any other relevant information?

## Checklist
- [ ] I have profiled the query (provide profile if possible)
- [ ] I have tested with the latest version
- [ ] I have provided benchmark comparison (if applicable)
