# Qdrant: vector ANN benchmark

- **Corpus:** 88,155 vectors × 1024 dimensions (amazon.titan-embed-text-v2:0, cosine), chunks of NornicDB, code-intelligence, soraban-api, soraban-connect, soraban-react-app, atlas-backend, atlas-frontend, soraban-infra (4,362 documentation, 83,793 code); 1,000 held-out queries; exact top-100 ground truth.
- **Measured:** recall@10 against exact ground truth; sequential client latency over 1,000 queries after 100 warm-up; throughput with 8 concurrent RPCs on one shared channel.
- **Engines:** NornicDB v1.4.3 (`7c201376d1203a651fc625869fb98df7be746ac4`), Qdrant 1.19.2 (native binary); one server at a time, each on a fresh data directory, both driven over the Qdrant gRPC API (Upsert batches of 500, Search).
- **Machine:** Apple M2 Max, 12 cores, 32 GiB, Darwin 25.5.0.
- **Provenance:** effective settings and binary/corpus SHA-256 hashes are recorded in each result JSON; resolved dependencies are in `environment.json`.
- **NornicDB GPU paths:** see each run's effective settings in its JSON; CPU-only runs disable the GPU manager, including GPU clustering.

## HNSW + product quantization x16 (rescore) [rescore8]

- **Strategy:** HNSW m=16 ef_construct=100 + product quantization
- **Load:** 4.4 s (20,189 vectors/s); **index ready:** 28.3 s; **disk:** 480 MB; **RSS:** 924 MB after load, 577 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9990 | 0.9988 | 0.663 ms | 0.651 ms | 0.761 ms | 0.875 ms | 4,009 |
| 16 | 0.9990 | 0.9988 | 0.668 ms | 0.645 ms | 0.806 ms | 1.035 ms | 3,975 |
| 32 | 0.9990 | 0.9988 | 0.654 ms | 0.644 ms | 0.758 ms | 0.869 ms | 4,034 |
| 64 | 0.9990 | 0.9988 | 0.654 ms | 0.647 ms | 0.745 ms | 0.838 ms | 3,935 |
| 128 | 0.9990 | 0.9993 | 0.721 ms | 0.714 ms | 0.823 ms | 0.912 ms | 3,577 |
| 256 | 1.0000 | 0.9999 | 0.908 ms | 0.904 ms | 1.034 ms | 1.141 ms | 2,500 |
| 512 | 1.0000 | 1.0000 | 1.230 ms | 1.231 ms | 1.424 ms | 1.557 ms | 2,277 |

## HNSW + binary quantization (rescore, 8x oversampling) [rescore8]

- **Strategy:** HNSW m=16 ef_construct=100 + binary quantization
- **Load:** 5.9 s (15,061 vectors/s); **index ready:** 1.0 s; **disk:** 453 MB; **RSS:** 2,787 MB after load, 594 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9980 | 0.9980 | 0.377 ms | 0.364 ms | 0.447 ms | 0.491 ms | 5,960 |
| 16 | 0.9980 | 0.9980 | 0.327 ms | 0.326 ms | 0.367 ms | 0.415 ms | 5,641 |
| 32 | 0.9980 | 0.9980 | 0.327 ms | 0.324 ms | 0.374 ms | 0.418 ms | 6,609 |
| 64 | 0.9980 | 0.9980 | 0.327 ms | 0.328 ms | 0.360 ms | 0.386 ms | 6,790 |
| 128 | 0.9990 | 0.9989 | 0.410 ms | 0.352 ms | 1.033 ms | 1.339 ms | 6,415 |
| 256 | 1.0000 | 0.9999 | 0.441 ms | 0.419 ms | 0.606 ms | 0.729 ms | 4,387 |
| 512 | 1.0000 | 0.9999 | 0.577 ms | 0.571 ms | 0.666 ms | 0.741 ms | 3,778 |

## HNSW + binary storage, int8 query (rescore, 8x oversampling) [rescore8]

- **Strategy:** HNSW m=16 ef_construct=100 + binary quantization
- **Load:** 5.8 s (15,301 vectors/s); **index ready:** 1.0 s; **disk:** 454 MB; **RSS:** 2,714 MB after load, 595 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9990 | 0.9990 | 0.407 ms | 0.382 ms | 0.479 ms | 0.557 ms | 5,908 |
| 16 | 0.9990 | 0.9990 | 0.353 ms | 0.351 ms | 0.409 ms | 0.483 ms | 6,885 |
| 32 | 0.9990 | 0.9990 | 0.332 ms | 0.330 ms | 0.370 ms | 0.389 ms | 6,637 |
| 64 | 0.9990 | 0.9990 | 0.333 ms | 0.332 ms | 0.369 ms | 0.388 ms | 5,841 |
| 128 | 1.0000 | 0.9995 | 0.363 ms | 0.362 ms | 0.405 ms | 0.428 ms | 6,471 |
| 256 | 1.0000 | 0.9998 | 0.469 ms | 0.449 ms | 0.644 ms | 0.723 ms | 3,823 |
| 512 | 1.0000 | 0.9999 | 0.606 ms | 0.599 ms | 0.704 ms | 0.784 ms | 4,021 |

## HNSW + product quantization x32 (rescore) [rescore8]

- **Strategy:** HNSW m=16 ef_construct=100 + product quantization
- **Load:** 4.4 s (20,166 vectors/s); **index ready:** 23.2 s; **disk:** 470 MB; **RSS:** 725 MB after load, 568 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9980 | 0.9978 | 0.515 ms | 0.505 ms | 0.593 ms | 0.667 ms | 5,217 |
| 16 | 0.9980 | 0.9978 | 0.507 ms | 0.500 ms | 0.585 ms | 0.662 ms | 5,106 |
| 32 | 0.9980 | 0.9978 | 0.471 ms | 0.464 ms | 0.554 ms | 0.618 ms | 5,196 |
| 64 | 0.9980 | 0.9978 | 0.503 ms | 0.497 ms | 0.592 ms | 0.683 ms | 5,079 |
| 128 | 0.9990 | 0.9986 | 0.547 ms | 0.540 ms | 0.641 ms | 0.725 ms | 3,821 |
| 256 | 0.9990 | 0.9995 | 0.654 ms | 0.647 ms | 0.755 ms | 0.859 ms | 3,951 |
| 512 | 1.0000 | 1.0000 | 0.822 ms | 0.816 ms | 0.954 ms | 1.056 ms | 3,062 |

## HNSW + product quantization x64 (rescore) [rescore8]

- **Strategy:** HNSW m=16 ef_construct=100 + product quantization
- **Load:** 4.4 s (20,073 vectors/s); **index ready:** 16.2 s; **disk:** 463 MB; **RSS:** 553 MB after load, 557 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9940 | 0.9909 | 0.430 ms | 0.411 ms | 0.537 ms | 0.609 ms | 6,077 |
| 16 | 0.9940 | 0.9909 | 0.413 ms | 0.404 ms | 0.486 ms | 0.537 ms | 5,109 |
| 32 | 0.9940 | 0.9909 | 0.445 ms | 0.439 ms | 0.521 ms | 0.580 ms | 4,738 |
| 64 | 0.9940 | 0.9909 | 0.454 ms | 0.444 ms | 0.542 ms | 0.608 ms | 5,996 |
| 128 | 0.9970 | 0.9940 | 0.473 ms | 0.466 ms | 0.559 ms | 0.629 ms | 5,442 |
| 256 | 0.9980 | 0.9966 | 0.533 ms | 0.524 ms | 0.631 ms | 0.716 ms | 4,678 |
| 512 | 0.9980 | 0.9974 | 0.689 ms | 0.676 ms | 0.835 ms | 0.952 ms | 3,809 |
