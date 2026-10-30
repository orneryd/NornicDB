# Graph-RAG: Typical Distributed vs NornicDB In-Memory

This document compares a **typical distributed Graph-RAG architecture** (separate services for embedding, reranking, vector store, and LLM) with **NornicDB’s unified in-process design**, and summarizes the **latency reduction** when all three model roles (embedding, reranker, inference) run in memory in a single process.

---

## Typical Distributed Graph-RAG (Reference)

Multiple network hops: Orchestrator → Tool Plugin → Embedding API → Vector Store → Reranker API → LLM. Each arrow implies serialization, network RTT, and often queueing.

```mermaid
flowchart LR
    subgraph Users
        U[("Users")]
    end

    subgraph Typical["Typical Graph-RAG (Distributed)"]
        direction TB
        Orch["Orchestrator"]
        Tool["Tool Plugin"]
        QE["Query Embedding<br/>TF-IDF + Embedding API"]
        VS["Vector Store<br/>(Qdrant)"]
        RRF["RRF + Adjacent Chunks"]
        Rerank["Reranker API<br/>bge-reranker-base"]
        LLM["LLM API<br/>(Meta/OpenAI)"]

        U -->|Query| Orch
        Orch -->|Query| Tool
        Tool -->|Query| QE
        QE -->|"Sparse + Dense Vec"| VS
        VS -->|Top k×2| RRF
        RRF -->|Chunks| Rerank
        Rerank -->|Top k + Metadata| Tool
        Tool --> Orch
        Orch -->|Context + Query| LLM
        LLM -->|Response| Orch
        Orch -->|Response| U
    end

    style QE fill:#ffeb3b,color:#000
    style Rerank fill:#ffeb3b,color:#000
    style LLM fill:#2196f3,color:#fff
    style VS fill:#9e9e9e,color:#fff
```

**Latency (typical ballpark per request):**

| Step | Service | Est. latency (network + compute) |
|------|---------|----------------------------------|
| 1 | Orchestrator → Tool Plugin | 1–5 ms |
| 2 | Query → Embedding API (e.g. FastAPI bge-small) | 20–80 ms |
| 3 | Vectors → Vector Store (Qdrant) retrieval | 10–50 ms |
| 4 | Chunks → Reranker API (e.g. FastAPI bge-reranker) | 30–100 ms |
| 5 | Reranked context → Orchestrator | 1–5 ms |
| 6 | Context + Query → LLM API (generation) | 200–2000+ ms |
| **Total (retrieval path)** | **~60–240 ms** (before LLM) | |
| **Total (full request)** | **~260–2240+ ms** | |

---

## NornicDB: Single-Process Graph-RAG (Simplified)

Embedding, vector search (+ optional BM25), reranking, and graph traversal live in the same process as the application. The inference LLM can be local (e.g. GGUF) in the same host or a separate API; when local, “all 3 LLMs” (embedder, reranker, inference) are in-memory / on-box.

```mermaid
flowchart LR
    subgraph Users
        U2[("Users")]
    end

    subgraph NornicDB["NornicDB (Single Process)"]
        direction TB
        App["App / Heimdall<br/>Orchestrator + Tool"]
        Embed["Embedding Model<br/>(in-memory, e.g. bge-m3)"]
        Store["Storage + Vector Index<br/>(Badger + HNSW/BM25)"]
        Rerank2["Reranker<br/>(in-memory, optional)"]
        Infer["Inference LLM<br/>(local GGUF or API)"]

        U2 -->|Query| App
        App -->|Query text| Embed
        Embed -->|Dense vec| Store
        Store -->|Top k and graph neighbors| Rerank2
        Rerank2 -->|Ranked chunks| App
        App -->|Context + Query| Infer
        Infer -->|Response| App
        App -->|Response| U2
    end

    style Embed fill:#4caf50,color:#fff
    style Rerank2 fill:#4caf50,color:#fff
    style Infer fill:#2196f3,color:#fff
    style Store fill:#795548,color:#fff
```

**Latency (in-process, no network between components):**

| Step | Component | Est. latency (in-process) |
|------|-----------|----------------------------|
| 1 | Query → Embedding (same process) | 1–15 ms |
| 2 | Vector + BM25 search (local index) | 0.5–5 ms |
| 3 | Graph traversal (depth 1, same store) | 0.5–3 ms |
| 4 | Rerank (same process, optional) | 2–20 ms |
| 5 | Context + Query → LLM (local or API) | 200–2000+ ms (unchanged if API) |
| **Total (retrieval path)** | **~4–43 ms** | |
| **Total (full request, local LLM)** | **~204–2043 ms** (LLM dominates) | |

---

## Side-by-Side Latency Comparison

```mermaid
flowchart TB
    subgraph Legend
        L1["🟡 Distributed: network + service hop"]
        L2["🟢 NornicDB: in-process"]
    end

    subgraph Distributed["Retrieval path (typical)"]
        D1["Orchestrator"] -->|"20–80 ms"| D2["Embedding API"]
        D2 -->|"10–50 ms"| D3["Vector Store"]
        D3 -->|"30–100 ms"| D4["Reranker API"]
        D4 --> D5["Back to Orchestrator"]
    end

    subgraph InProcess["Retrieval path (NornicDB)"]
        N1["App"] -->|"1–15 ms"| N2["Embedding (in-mem)"]
        N2 -->|"0.5–5 ms"| N3["Vector Index"]
        N3 -->|"2–20 ms"| N4["Reranker (in-mem)"]
        N4 --> N5["Back to App"]
    end

    Distributed -.->|"~60–240 ms total"| Summary1["Retrieval total"]
    InProcess -.->|"~4–43 ms total"| Summary2["Retrieval total"]
```

| Metric | Typical distributed | NornicDB in-memory |
|--------|----------------------|--------------------|
| **Retrieval path (embed → search → rerank)** | ~60–240 ms | ~4–43 ms |
| **Latency reduction (retrieval)** | — | **~5–15× lower** (ballpark) |
| **Network hops (retrieval)** | 3–4 (embed, store, rerank) | 0 |
| **All 3 “LLMs” (embed, rerank, infer)** | Separate services/APIs | Embed + rerank in-process; infer local or API |

---

## Deployment: Containers and Services

Typical Graph-RAG uses **multiple discrete services**, each often run in its own container with its own process, networking, and scaling. NornicDB collapses retrieval (embedding, vector store, reranker, graph) into **one in-memory process** deployable as a **single Docker container**.

### Typical Graph-RAG: Multiple Containers

Each logical service typically runs in a separate container; the orchestrator and tool plugin may share a container, but embedding, vector store, reranker, and (if self-hosted) the LLM each add at least one container.

```mermaid
flowchart TB
    subgraph Host["Single host (e.g. Docker Compose)"]
        subgraph C1["Container 1: App / Orchestrator"]
            Orch["Orchestrator + Tool Plugin"]
        end
        subgraph C2["Container 2: Embedding API"]
            EmbedSvc["FastAPI<br/>bge-small / bge-m3"]
        end
        subgraph C3["Container 3: Vector Store"]
            Qdrant["Qdrant<br/>Vector DB"]
        end
        subgraph C4["Container 4: Reranker API"]
            RerankSvc["FastAPI<br/>bge-reranker-base"]
        end
        subgraph C5["Container 5: LLM (if self-hosted)"]
            LLMSvc["vLLM / Ollama / etc."]
        end
    end

    Orch <-->|HTTP/gRPC| EmbedSvc
    Orch <-->|gRPC| Qdrant
    Orch <-->|HTTP| RerankSvc
    Orch <-->|HTTP| LLMSvc

    style C1 fill:#e3f2fd
    style C2 fill:#fff3e0
    style C3 fill:#f5f5f5
    style C4 fill:#fff3e0
    style C5 fill:#e8f5e9
```

| # | Container / service | Role |
|---|----------------------|------|
| 1 | App / Orchestrator | Request handling, tool plugin, orchestration |
| 2 | Embedding API (e.g. FastAPI) | Query + chunk embedding (bge-small / bge-m3) |
| 3 | Vector Store (e.g. Qdrant) | Vector + optional sparse index, persistence |
| 4 | Reranker API (e.g. FastAPI) | Cross-encoder reranking (bge-reranker) |
| 5 | LLM (if self-hosted) | vLLM, Ollama, or similar for generation |
| **Total** | **5 containers** (4 if LLM is external API) | |

### NornicDB: Single Container, Single Process

All retrieval components run in **one process** inside **one container**: embedding model, vector/BM25 index, optional reranker, graph storage, and (if configured) local inference LLM. No inter-container networking for the retrieval path.

```mermaid
flowchart TB
    subgraph Single["Single container: NornicDB"]
        subgraph Process["One process (in-memory)"]
            App2["App / Heimdall<br/>Orchestrator + Tool"]
            Embed2["Embedding<br/>(in-memory)"]
            Store2["Storage + Vector Index<br/>Badger + HNSW + BM25"]
            Rerank2["Reranker<br/>(in-memory, optional)"]
            Infer2["Inference LLM<br/>(local GGUF or outbound API)"]
            App2 --> Embed2
            Embed2 --> Store2
            Store2 --> Rerank2
            Rerank2 --> App2
            App2 --> Infer2
            Infer2 --> App2
        end
    end

    style Process fill:#c8e6c9
```

| # | Container / process | Contents |
|---|----------------------|----------|
| 1 | **NornicDB** (single container, single process) | Orchestrator, Tool Plugin, Embedding, Vector Index + BM25, Graph Store (Badger), Reranker (optional), local LLM (optional) |
| **Total** | **1 container** | All retrieval + optional local inference in one deployable unit |

### Side-by-Side: Container Count and Complexity

```mermaid
flowchart LR
    subgraph TypicalDeploy["Typical Graph-RAG deployment"]
        direction TB
        T1["📦 Container 1<br/>Orchestrator"]
        T2["📦 Container 2<br/>Embedding API"]
        T3["📦 Container 3<br/>Qdrant"]
        T4["📦 Container 4<br/>Reranker API"]
        T5["📦 Container 5<br/>LLM (optional)"]
    end

    subgraph NornicDeploy["NornicDB deployment"]
        N1["📦 Single container<br/>Embed + Store + Rerank + App<br/>(+ optional local LLM)"]
    end

    TypicalDeploy -->|"5 containers, multi-service config, inter-container network"| L1[Typical]
    NornicDeploy -->|"1 container, single process, no retrieval network hops"| L2[NornicDB]
```

| Aspect | Typical Graph-RAG | NornicDB |
|--------|--------------------|----------|
| **Containers (min)** | 4 (orchestrator, embedding, vector store, reranker) | 1 |
| **Containers (with self-hosted LLM)** | 5 | 1 |
| **Processes (retrieval path)** | 4+ (one per service) | 1 |
| **Inter-service networking** | Yes (HTTP/gRPC between containers) | No (in-process only) |
| **Config / env** | Multiple images, ports, envs, health checks | Single image, one port, one env |
| **Scaling** | Scale each service independently (more ops) | Scale single container (simpler) |

---

## Summary

- **Typical Graph-RAG:** Orchestrator, Tool Plugin, Embedding API, Vector Store (e.g. Qdrant), Reranker API, and LLM API are separate; each step adds network and serialization cost. **Deployment** usually means **4–5 containers** (orchestrator, embedding API, vector store, reranker, and optionally self-hosted LLM), each with its own image, port, and config.
- **NornicDB:** Embedding model, vector/BM25 index, optional reranker, and graph storage run in **one process**; retrieval is in-process and much faster. When inference is also local (e.g. GGUF), all three model roles (embedding, reranking, inference) are in-memory on the same machine. **Deployment** is a **single Docker container** (one process, one port, no inter-container networking for retrieval).
- **Latency:** Retrieval path drops from roughly **60–240 ms** to **4–43 ms** in the NornicDB case; end-to-end latency is then dominated by the inference LLM (same as in the distributed setup if both use the same LLM API or local model).
- **Ops simplification:** One container and one process to deploy, scale, and monitor instead of 4–5; no retrieval-path networking or cross-service health checks.
