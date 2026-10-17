# Qdrant: vector ANN benchmark

- **Corpus:** 88,155 vectors × 1024 dimensions (amazon.titan-embed-text-v2:0, cosine), chunks of NornicDB, code-intelligence, soraban-api, soraban-connect, soraban-react-app, atlas-backend, atlas-frontend, soraban-infra (4,362 documentation, 83,793 code); 1,000 held-out queries; exact top-100 ground truth.
- **Measured:** recall@10 against exact ground truth; sequential client latency over 1,000 queries after 100 warm-up; throughput with 8 concurrent RPCs on one shared channel.
- **Engines:** NornicDB v1.4.3 (`5f7e301634bd9e209987f26b4997b88488027837`), Qdrant 1.19.2 (native binary); one server at a time, each on a fresh data directory, both driven over the Qdrant gRPC API (Upsert batches of 500, Search).
- **Machine:** Apple M2 Max, 12 cores, 32 GiB, Darwin 25.5.0.
- **Provenance:** effective settings and binary/corpus SHA-256 hashes are recorded in each result JSON; resolved dependencies are in `environment.json`.
- **NornicDB GPU paths:** see each run's effective settings in its JSON; CPU-only runs disable the GPU manager, including GPU clustering.

## Exact (brute force)

- **Strategy:** exact search (exact=true)
- **Load:** 5.3 s (16,778 vectors/s); **index ready:** 1.0 s; **disk:** 436 MB; **RSS:** 3,016 MB after load, 903 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 1.0000 | 1.0000 | 3.823 ms | 3.797 ms | 4.042 ms | 4.701 ms | 376 |

## HNSW fast (M=16, efConstruction=100, efSearch=50)

- **Strategy:** HNSW m=16 ef_construct=100
- **Load:** 5.3 s (16,507 vectors/s); **index ready:** 4.0 s; **disk:** 440 MB; **RSS:** 2,129 MB after load, 543 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9980 | 0.9975 | 0.571 ms | 0.573 ms | 0.667 ms | 0.717 ms | 3,645 |
| 16 | 0.9900 | 0.9821 | 0.420 ms | 0.416 ms | 0.486 ms | 0.528 ms | 6,075 |
| 32 | 0.9960 | 0.9940 | 0.517 ms | 0.516 ms | 0.591 ms | 0.627 ms | 4,763 |
| 64 | 0.9980 | 0.9982 | 0.686 ms | 0.688 ms | 0.793 ms | 0.854 ms | 3,284 |
| 128 | 1.0000 | 0.9997 | 0.975 ms | 0.983 ms | 1.145 ms | 1.231 ms | 2,079 |
| 256 | 1.0000 | 1.0000 | 1.472 ms | 1.483 ms | 1.736 ms | 1.849 ms | 1,298 |
| 512 | 1.0000 | 1.0000 | 2.388 ms | 2.373 ms | 2.871 ms | 3.715 ms | 815 |

## HNSW balanced (M=16, efConstruction=200, efSearch=100)

- **Strategy:** HNSW m=16 ef_construct=200
- **Load:** 5.2 s (16,874 vectors/s); **index ready:** 9.1 s; **disk:** 442 MB; **RSS:** 1,051 MB after load, 542 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 1.0000 | 0.9994 | 0.925 ms | 0.927 ms | 1.070 ms | 1.142 ms | 2,257 |
| 16 | 0.9850 | 0.9841 | 0.443 ms | 0.440 ms | 0.508 ms | 0.538 ms | 5,928 |
| 32 | 0.9940 | 0.9957 | 0.563 ms | 0.561 ms | 0.646 ms | 0.693 ms | 4,314 |
| 64 | 0.9990 | 0.9989 | 0.719 ms | 0.719 ms | 0.847 ms | 0.903 ms | 2,980 |
| 128 | 1.0000 | 0.9996 | 1.096 ms | 1.100 ms | 1.271 ms | 1.346 ms | 1,934 |
| 256 | 1.0000 | 0.9999 | 1.675 ms | 1.684 ms | 1.977 ms | 2.147 ms | 1,202 |
| 512 | 1.0000 | 0.9999 | 2.710 ms | 2.733 ms | 3.247 ms | 3.440 ms | 763 |

## HNSW accurate (M=32, efConstruction=400, efSearch=200)

- **Strategy:** HNSW m=32 ef_construct=400
- **Load:** 5.3 s (16,478 vectors/s); **index ready:** 20.2 s; **disk:** 443 MB; **RSS:** 747 MB after load, 544 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 1.0000 | 1.0000 | 1.976 ms | 1.990 ms | 2.351 ms | 2.477 ms | 1,026 |
| 16 | 0.9970 | 0.9906 | 0.495 ms | 0.493 ms | 0.580 ms | 0.633 ms | 4,377 |
| 32 | 0.9990 | 0.9978 | 0.685 ms | 0.685 ms | 0.787 ms | 0.845 ms | 3,408 |
| 64 | 1.0000 | 0.9996 | 0.987 ms | 0.994 ms | 1.153 ms | 1.209 ms | 2,181 |
| 128 | 1.0000 | 0.9999 | 1.516 ms | 1.525 ms | 1.787 ms | 1.915 ms | 1,332 |
| 256 | 1.0000 | 1.0000 | 2.363 ms | 2.351 ms | 2.811 ms | 4.405 ms | 876 |
| 512 | 1.0000 | 1.0000 | 3.590 ms | 3.635 ms | 4.297 ms | 4.602 ms | 575 |

## HNSW + scalar int8 quantization (rescore)

- **Strategy:** HNSW m=16 ef_construct=100 + scalar quantization
- **Load:** 5.0 s (17,686 vectors/s); **index ready:** 1.0 s; **disk:** 531 MB; **RSS:** 2,510 MB after load, 673 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9970 | 0.9960 | 0.307 ms | 0.303 ms | 0.342 ms | 0.357 ms | 6,976 |
| 16 | 0.9910 | 0.9842 | 0.266 ms | 0.266 ms | 0.293 ms | 0.308 ms | 7,284 |
| 32 | 0.9950 | 0.9945 | 0.285 ms | 0.285 ms | 0.319 ms | 0.332 ms | 7,389 |
| 64 | 0.9990 | 0.9972 | 0.321 ms | 0.319 ms | 0.362 ms | 0.385 ms | 6,658 |
| 128 | 1.0000 | 0.9993 | 0.417 ms | 0.406 ms | 0.509 ms | 0.678 ms | 5,089 |
| 256 | 1.0000 | 0.9997 | 0.607 ms | 0.580 ms | 0.744 ms | 1.334 ms | 4,482 |
| 512 | 1.0000 | 0.9999 | 0.796 ms | 0.796 ms | 0.919 ms | 0.972 ms | 2,634 |

## HNSW + product quantization x16 (rescore)

- **Strategy:** HNSW m=16 ef_construct=100 + product quantization
- **Load:** 4.5 s (19,754 vectors/s); **index ready:** 28.2 s; **disk:** 480 MB; **RSS:** 948 MB after load, 577 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9970 | 0.9950 | 0.532 ms | 0.522 ms | 0.603 ms | 0.664 ms | 4,805 |
| 16 | 0.9890 | 0.9808 | 0.504 ms | 0.501 ms | 0.557 ms | 0.601 ms | 5,548 |
| 32 | 0.9940 | 0.9913 | 0.542 ms | 0.537 ms | 0.602 ms | 0.655 ms | 4,439 |
| 64 | 0.9970 | 0.9956 | 0.592 ms | 0.587 ms | 0.667 ms | 0.739 ms | 4,517 |
| 128 | 0.9990 | 0.9974 | 0.671 ms | 0.667 ms | 0.770 ms | 0.855 ms | 3,847 |
| 256 | 0.9990 | 0.9977 | 0.873 ms | 0.870 ms | 0.999 ms | 1.068 ms | 3,151 |
| 512 | 1.0000 | 0.9978 | 1.169 ms | 1.172 ms | 1.353 ms | 1.457 ms | 2,473 |

## HNSW + binary quantization (rescore, 2x oversampling)

- **Strategy:** HNSW m=16 ef_construct=100 + binary quantization
- **Load:** 5.6 s (15,661 vectors/s); **index ready:** 1.0 s; **disk:** 1,071 MB; **RSS:** 2,709 MB after load, 1,433 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9970 | 0.9944 | 0.287 ms | 0.282 ms | 0.320 ms | 0.341 ms | 7,625 |
| 16 | 0.9880 | 0.9823 | 0.270 ms | 0.269 ms | 0.302 ms | 0.320 ms | 7,376 |
| 32 | 0.9900 | 0.9893 | 0.269 ms | 0.268 ms | 0.301 ms | 0.318 ms | 7,529 |
| 64 | 0.9970 | 0.9954 | 0.290 ms | 0.290 ms | 0.322 ms | 0.341 ms | 7,422 |
| 128 | 0.9970 | 0.9978 | 0.321 ms | 0.321 ms | 0.358 ms | 0.374 ms | 7,030 |
| 256 | 1.0000 | 0.9987 | 0.382 ms | 0.381 ms | 0.425 ms | 0.454 ms | 6,392 |
| 512 | 1.0000 | 0.9989 | 0.498 ms | 0.496 ms | 0.574 ms | 0.643 ms | 4,686 |
