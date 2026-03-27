<p align="center">
  <img src="https://raw.githubusercontent.com/orneryd/NornicDB/refs/heads/main/docs/assets/logos/nornicdb-logo.svg" alt="NornicDB Logo" width="200"/>
</p>

<h1 align="center">NornicDB</h1>

<p align="center">
  <strong>The Graph Database That Learns</strong><br/>
  Neo4j-compatible • GPU-accelerated • Memory that evolves
</p>

<p align="center">
  <img src="https://img.shields.io/badge/version-1.0.34-success" alt="Version 1.0.34">
  <a href="https://coveralls.io/github/orneryd/NornicDB?branch=main"><img src="https://coveralls.io/repos/github/orneryd/NornicDB/badge.svg?branch=main" alt="Coveralls Report"></a>
  <a href="https://hub.docker.com/u/timothyswt"><img src="https://img.shields.io/badge/docker-ready-blue?logo=docker" alt="Docker"></a>
  <a href="https://neo4j.com/"><img src="https://img.shields.io/badge/neo4j-compatible-008CC1?logo=neo4j" alt="Neo4j Compatible"></a>
  <a href="https://github.com/qdrant/qdrant"><img src="https://img.shields.io/badge/qdrant-compatible-008CC1?logo=qdrant" alt="Qdrant Compatible Compatible"></a>
  <a href="https://go.dev/"><img src="https://img.shields.io/badge/go-%3E%3D1.26-00ADD8?logo=go" alt="Go Version"></a>
  <a href="https://goreportcard.com/report/github.com/orneryd/nornicdb"><img src="https://goreportcard.com/badge/github.com/orneryd/nornicdb" alt="Go Report Card"></a>
  <a href="LICENSE.md"><img src="https://img.shields.io/badge/license-MIT-blue" alt="License"></a>
</p>
<p align="center">
  <a href="https://discord.gg/yszYHrxp4N"><img src="https://img.shields.io/badge/discord-community-00ADD8?logo=discord" alt="Discord Community Server"></a>
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a> •
  <a href="#why-switch-from-neo4j">Why Switch</a> •
  <a href="#what-problem-does-this-solve">Problem</a> •
  <a href="#why-nornicdb-is-different">Why Different</a> •
  <a href="#performance-snapshot">Benchmarks</a> •
  <a href="#features">Features</a> •
  <a href="#docker-images">Docker</a> •
  <a href="#documentation">Docs</a> •
  <a href="#contributors">Contributors</a>
</p>

## Try It With One Command

```bash
# arm64 / Apple Silicon
docker run -d --name nornicdb -p 7474:7474 -p 7687:7687 -v nornicdb-data:/data timothyswt/nornicdb-arm64-metal-bge:latest

# amd64 / CPU only
docker run -d --name nornicdb -p 7474:7474 -p 7687:7687 -v nornicdb-data:/data timothyswt/nornicdb-amd64-cpu-bge:latest
```

Open [http://localhost:7474](http://localhost:7474) for the admin UI. For NVIDIA CUDA hosts, use `timothyswt/nornicdb-amd64-cuda-bge:latest`. For Vulkan hosts, use `timothyswt/nornicdb-amd64-vulkan-bge:latest`.

---

## What Problem Does This Solve?

NornicDB is a high-performance graph database designed for AI agents and knowledge systems. It speaks Neo4j's language (Bolt protocol + Cypher) so you can switch with zero code changes, while adding intelligent features that traditional databases lack.

NornicDB automatically discovers and manages relationships in your data, weaving conn

## Contributing

We welcome contributions! To get started:
1. Fork the repository and create your feature branch.
2. Ensure your code passes `go test ./...` and follows Go standards.
3. Submit a Pull Request with a clear description of your changes.
For major changes, please open an issue first to discuss what you would like to change.