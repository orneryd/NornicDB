# Qdrant: vector ANN benchmark

- **Corpus:** 88,155 vectors × 1024 dimensions (amazon.titan-embed-text-v2:0, cosine), chunks of NornicDB, code-intelligence, soraban-api, soraban-connect, soraban-react-app, atlas-backend, atlas-frontend, soraban-infra (4,362 documentation, 83,793 code); 1,000 held-out queries; exact top-100 ground truth.
- **Measured:** recall@10 against exact ground truth; sequential client latency over 1,000 queries after 100 warm-up; throughput with 8 concurrent RPCs on one shared channel.
- **Engines:** NornicDB v1.4.3 (`ce38410589d30884699cf0a61b871f51dfe3f493`), Qdrant 1.19.2 (native binary); one server at a time, each on a fresh data directory, both driven over the Qdrant gRPC API (Upsert batches of 500, Search).
- **Machine:** Apple M2 Max, 12 cores, 32 GiB, Darwin 25.5.0.
- **Provenance:** effective settings and binary/corpus SHA-256 hashes are recorded in each result JSON; resolved dependencies are in `environment.json`.
- **NornicDB GPU paths:** see each run's effective settings in its JSON; CPU-only runs disable the GPU manager, including GPU clustering.

## Exact (brute force)

- **Strategy:** exact search (exact=true)
- **Load:** 5.3 s (16,519 vectors/s); **index ready:** 1.0 s; **disk:** 436 MB; **RSS:** 2,943 MB after load, 901 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 1.0000 | 1.0000 | 4.145 ms | 4.075 ms | 5.028 ms | 5.155 ms | 356 |

## HNSW fast (M=16, efConstruction=100, efSearch=50)

- **Strategy:** HNSW m=16 ef_construct=100
- **Load:** 5.7 s (15,534 vectors/s); **index ready:** 4.0 s; **disk:** 440 MB; **RSS:** 2,067 MB after load, 540 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9990 | 0.9966 | 0.630 ms | 0.625 ms | 0.735 ms | 0.777 ms | 3,615 |
| 16 | 0.9910 | 0.9817 | 0.432 ms | 0.428 ms | 0.500 ms | 0.546 ms | 5,830 |
| 32 | 0.9960 | 0.9931 | 0.501 ms | 0.499 ms | 0.583 ms | 0.616 ms | 4,337 |
| 64 | 0.9990 | 0.9971 | 0.699 ms | 0.702 ms | 0.808 ms | 0.867 ms | 3,051 |
| 128 | 0.9990 | 0.9990 | 0.976 ms | 0.985 ms | 1.142 ms | 1.213 ms | 1,912 |
| 256 | 1.0000 | 0.9998 | 1.497 ms | 1.512 ms | 1.763 ms | 1.867 ms | 1,214 |
| 512 | 1.0000 | 0.9998 | 2.309 ms | 2.324 ms | 2.752 ms | 2.897 ms | 815 |

## HNSW balanced (M=16, efConstruction=200, efSearch=100)

- **Strategy:** HNSW m=16 ef_construct=200
- **Load:** 5.5 s (16,045 vectors/s); **index ready:** 10.1 s; **disk:** 442 MB; **RSS:** 547 MB after load, 542 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 1.0000 | 0.9997 | 0.959 ms | 0.963 ms | 1.103 ms | 1.143 ms | 2,053 |
| 16 | 0.9930 | 0.9856 | 0.450 ms | 0.447 ms | 0.523 ms | 0.550 ms | 5,716 |
| 32 | 0.9950 | 0.9946 | 0.511 ms | 0.511 ms | 0.585 ms | 0.637 ms | 4,414 |
| 64 | 0.9970 | 0.9982 | 0.755 ms | 0.759 ms | 0.867 ms | 0.917 ms | 2,975 |
| 128 | 1.0000 | 0.9999 | 1.077 ms | 1.084 ms | 1.270 ms | 1.332 ms | 1,893 |
| 256 | 1.0000 | 1.0000 | 1.693 ms | 1.706 ms | 2.003 ms | 2.134 ms | 1,185 |
| 512 | 1.0000 | 1.0000 | 2.700 ms | 2.719 ms | 3.222 ms | 3.446 ms | 770 |

## HNSW accurate (M=32, efConstruction=400, efSearch=200)

- **Strategy:** HNSW m=32 ef_construct=400
- **Load:** 5.7 s (15,355 vectors/s); **index ready:** 20.2 s; **disk:** 443 MB; **RSS:** 535 MB after load, 545 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 1.0000 | 1.0000 | 1.961 ms | 1.972 ms | 2.314 ms | 2.483 ms | 912 |
| 16 | 0.9970 | 0.9928 | 0.534 ms | 0.527 ms | 0.628 ms | 0.707 ms | 4,254 |
| 32 | 0.9990 | 0.9975 | 0.671 ms | 0.671 ms | 0.783 ms | 0.817 ms | 3,399 |
| 64 | 1.0000 | 0.9997 | 0.976 ms | 0.981 ms | 1.130 ms | 1.200 ms | 2,159 |
| 128 | 1.0000 | 1.0000 | 1.490 ms | 1.500 ms | 1.767 ms | 1.885 ms | 1,355 |
| 256 | 1.0000 | 1.0000 | 2.348 ms | 2.313 ms | 2.742 ms | 4.960 ms | 853 |
| 512 | 1.0000 | 1.0000 | 3.572 ms | 3.569 ms | 4.318 ms | 5.186 ms | 554 |

## HNSW + scalar int8 quantization (rescore)

- **Strategy:** HNSW m=16 ef_construct=100 + scalar quantization
- **Load:** 5.2 s (16,966 vectors/s); **index ready:** 2.0 s; **disk:** 531 MB; **RSS:** 2,512 MB after load, 660 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9970 | 0.9972 | 0.354 ms | 0.349 ms | 0.393 ms | 0.438 ms | 6,720 |
| 16 | 0.9920 | 0.9807 | 0.278 ms | 0.276 ms | 0.309 ms | 0.329 ms | 7,140 |
| 32 | 0.9950 | 0.9935 | 0.298 ms | 0.298 ms | 0.328 ms | 0.346 ms | 7,443 |
| 64 | 0.9970 | 0.9977 | 0.333 ms | 0.333 ms | 0.366 ms | 0.396 ms | 6,440 |
| 128 | 1.0000 | 0.9994 | 0.409 ms | 0.409 ms | 0.459 ms | 0.491 ms | 5,382 |
| 256 | 1.0000 | 0.9998 | 0.561 ms | 0.558 ms | 0.689 ms | 0.773 ms | 3,902 |
| 512 | 1.0000 | 1.0000 | 0.821 ms | 0.817 ms | 0.938 ms | 1.049 ms | 2,563 |

## HNSW + product quantization x16 (rescore)

- **Strategy:** HNSW m=16 ef_construct=100 + product quantization
- **Load:** 4.3 s (20,691 vectors/s); **index ready:** 30.3 s; **disk:** 480 MB; **RSS:** 906 MB after load, 574 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9980 | 0.9944 | 0.567 ms | 0.560 ms | 0.630 ms | 0.694 ms | 4,556 |
| 16 | 0.9890 | 0.9795 | 0.451 ms | 0.448 ms | 0.507 ms | 0.564 ms | 5,540 |
| 32 | 0.9960 | 0.9903 | 0.528 ms | 0.526 ms | 0.585 ms | 0.631 ms | 4,571 |
| 64 | 1.0000 | 0.9962 | 0.581 ms | 0.580 ms | 0.653 ms | 0.710 ms | 4,540 |
| 128 | 1.0000 | 0.9965 | 0.689 ms | 0.673 ms | 0.820 ms | 0.987 ms | 3,914 |
| 256 | 1.0000 | 0.9972 | 0.860 ms | 0.859 ms | 0.984 ms | 1.054 ms | 3,177 |
| 512 | 1.0000 | 0.9973 | 1.168 ms | 1.168 ms | 1.348 ms | 1.450 ms | 2,398 |

## HNSW + binary quantization (rescore, 2x oversampling)

- **Strategy:** HNSW m=16 ef_construct=100 + binary quantization
- **Load:** 6.0 s (14,709 vectors/s); **index ready:** 1.0 s; **disk:** 452 MB; **RSS:** 2,793 MB after load, 594 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9990 | 0.9952 | 0.294 ms | 0.288 ms | 0.326 ms | 0.368 ms | 7,403 |
| 16 | 0.9960 | 0.9825 | 0.271 ms | 0.272 ms | 0.301 ms | 0.317 ms | 7,221 |
| 32 | 0.9980 | 0.9915 | 0.279 ms | 0.279 ms | 0.308 ms | 0.330 ms | 7,271 |
| 64 | 0.9990 | 0.9962 | 0.303 ms | 0.300 ms | 0.345 ms | 0.396 ms | 7,140 |
| 128 | 0.9990 | 0.9975 | 0.330 ms | 0.330 ms | 0.371 ms | 0.399 ms | 6,782 |
| 256 | 0.9990 | 0.9984 | 0.391 ms | 0.391 ms | 0.439 ms | 0.462 ms | 5,385 |
| 512 | 0.9990 | 0.9985 | 0.511 ms | 0.500 ms | 0.630 ms | 0.775 ms | 4,435 |
