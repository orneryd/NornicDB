# Qdrant: vector ANN benchmark

- **Corpus:** 88,155 vectors × 1024 dimensions (amazon.titan-embed-text-v2:0, cosine), chunks of NornicDB, code-intelligence, soraban-api, soraban-connect, soraban-react-app, atlas-backend, atlas-frontend, soraban-infra (4,362 documentation, 83,793 code); 1,000 held-out queries; exact top-100 ground truth.
- **Measured:** recall@10 against exact ground truth; sequential client latency over 1,000 queries after 100 warm-up; throughput with 8 concurrent RPCs on one shared channel.
- **Engines:** NornicDB v1.4.3 (`7c201376d1203a651fc625869fb98df7be746ac4`), Qdrant 1.19.2 (native binary); one server at a time, each on a fresh data directory, both driven over the Qdrant gRPC API (Upsert batches of 500, Search).
- **Machine:** Apple M2 Max, 12 cores, 32 GiB, Darwin 25.5.0.
- **Provenance:** effective settings and binary/corpus SHA-256 hashes are recorded in each result JSON; resolved dependencies are in `environment.json`.
- **NornicDB GPU paths:** see each run's effective settings in its JSON; CPU-only runs disable the GPU manager, including GPU clustering.

## Exact (brute force)

- **Strategy:** exact search (exact=true)
- **Load:** 5.3 s (16,623 vectors/s); **index ready:** 1.0 s; **disk:** 437 MB; **RSS:** 2,964 MB after load, 902 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 1.0000 | 1.0000 | 4.001 ms | 3.870 ms | 5.000 ms | 5.129 ms | 369 |

## HNSW fast (M=16, efConstruction=100, efSearch=50)

- **Strategy:** HNSW m=16 ef_construct=100
- **Load:** 5.4 s (16,259 vectors/s); **index ready:** 4.1 s; **disk:** 440 MB; **RSS:** 2,060 MB after load, 539 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9990 | 0.9977 | 0.586 ms | 0.580 ms | 0.714 ms | 0.830 ms | 3,318 |
| 16 | 0.9920 | 0.9831 | 0.422 ms | 0.417 ms | 0.493 ms | 0.542 ms | 6,064 |
| 32 | 0.9950 | 0.9940 | 0.522 ms | 0.517 ms | 0.606 ms | 0.661 ms | 3,737 |
| 64 | 0.9990 | 0.9988 | 0.673 ms | 0.674 ms | 0.783 ms | 0.855 ms | 3,301 |
| 128 | 1.0000 | 0.9998 | 0.977 ms | 0.984 ms | 1.138 ms | 1.210 ms | 2,049 |
| 256 | 1.0000 | 1.0000 | 1.498 ms | 1.510 ms | 1.791 ms | 1.914 ms | 1,214 |
| 512 | 1.0000 | 1.0000 | 2.302 ms | 2.311 ms | 2.742 ms | 2.960 ms | 812 |

## HNSW balanced (M=16, efConstruction=200, efSearch=100)

- **Strategy:** HNSW m=16 ef_construct=200
- **Load:** 5.2 s (16,820 vectors/s); **index ready:** 11.1 s; **disk:** 442 MB; **RSS:** 698 MB after load, 538 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9990 | 0.9993 | 0.928 ms | 0.931 ms | 1.070 ms | 1.153 ms | 1,976 |
| 16 | 0.9930 | 0.9834 | 0.437 ms | 0.434 ms | 0.498 ms | 0.539 ms | 4,318 |
| 32 | 0.9960 | 0.9942 | 0.551 ms | 0.549 ms | 0.636 ms | 0.686 ms | 4,419 |
| 64 | 0.9990 | 0.9987 | 0.721 ms | 0.723 ms | 0.832 ms | 0.899 ms | 2,813 |
| 128 | 1.0000 | 0.9996 | 1.092 ms | 1.098 ms | 1.285 ms | 1.382 ms | 1,901 |
| 256 | 1.0000 | 0.9999 | 1.683 ms | 1.688 ms | 1.976 ms | 2.135 ms | 1,187 |
| 512 | 1.0000 | 0.9999 | 2.684 ms | 2.687 ms | 3.173 ms | 3.524 ms | 747 |

## HNSW accurate (M=32, efConstruction=400, efSearch=200)

- **Strategy:** HNSW m=32 ef_construct=400
- **Load:** 5.3 s (16,540 vectors/s); **index ready:** 20.2 s; **disk:** 443 MB; **RSS:** 539 MB after load, 542 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 1.0000 | 1.0000 | 2.019 ms | 2.030 ms | 2.395 ms | 2.591 ms | 1,009 |
| 16 | 0.9950 | 0.9923 | 0.535 ms | 0.532 ms | 0.622 ms | 0.682 ms | 4,284 |
| 32 | 0.9990 | 0.9984 | 0.673 ms | 0.676 ms | 0.779 ms | 0.852 ms | 3,367 |
| 64 | 0.9990 | 0.9994 | 0.973 ms | 0.976 ms | 1.144 ms | 1.206 ms | 2,088 |
| 128 | 1.0000 | 0.9998 | 1.492 ms | 1.511 ms | 1.762 ms | 1.901 ms | 1,328 |
| 256 | 1.0000 | 1.0000 | 2.323 ms | 2.343 ms | 2.754 ms | 2.948 ms | 861 |
| 512 | 1.0000 | 1.0000 | 3.586 ms | 3.611 ms | 4.245 ms | 4.568 ms | 565 |

## HNSW + scalar int8 quantization (rescore)

- **Strategy:** HNSW m=16 ef_construct=100 + scalar quantization
- **Load:** 5.0 s (17,642 vectors/s); **index ready:** 2.0 s; **disk:** 531 MB; **RSS:** 2,377 MB after load, 675 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9990 | 0.9966 | 0.370 ms | 0.349 ms | 0.458 ms | 1.061 ms | 7,156 |
| 16 | 0.9930 | 0.9840 | 0.274 ms | 0.272 ms | 0.306 ms | 0.323 ms | 7,419 |
| 32 | 0.9970 | 0.9941 | 0.295 ms | 0.294 ms | 0.328 ms | 0.342 ms | 6,118 |
| 64 | 0.9990 | 0.9977 | 0.330 ms | 0.329 ms | 0.370 ms | 0.391 ms | 6,909 |
| 128 | 1.0000 | 0.9992 | 0.403 ms | 0.401 ms | 0.460 ms | 0.487 ms | 5,951 |
| 256 | 1.0000 | 0.9997 | 0.579 ms | 0.567 ms | 0.716 ms | 0.868 ms | 3,926 |
| 512 | 1.0000 | 0.9999 | 0.801 ms | 0.799 ms | 0.923 ms | 0.979 ms | 2,698 |

## HNSW + product quantization x16 (rescore)

- **Strategy:** HNSW m=16 ef_construct=100 + product quantization
- **Load:** 4.3 s (20,422 vectors/s); **index ready:** 28.3 s; **disk:** 480 MB; **RSS:** 909 MB after load, 579 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9980 | 0.9936 | 0.565 ms | 0.558 ms | 0.621 ms | 0.686 ms | 4,783 |
| 16 | 0.9890 | 0.9783 | 0.494 ms | 0.467 ms | 0.607 ms | 0.823 ms | 5,421 |
| 32 | 0.9960 | 0.9906 | 0.511 ms | 0.507 ms | 0.576 ms | 0.624 ms | 5,103 |
| 64 | 0.9990 | 0.9947 | 0.583 ms | 0.580 ms | 0.652 ms | 0.725 ms | 4,527 |
| 128 | 1.0000 | 0.9960 | 0.686 ms | 0.683 ms | 0.766 ms | 0.827 ms | 3,711 |
| 256 | 1.0000 | 0.9966 | 0.851 ms | 0.849 ms | 0.960 ms | 1.041 ms | 2,982 |
| 512 | 1.0000 | 0.9967 | 1.248 ms | 1.213 ms | 1.510 ms | 1.871 ms | 2,329 |

## HNSW + binary quantization (rescore, 2x oversampling)

- **Strategy:** HNSW m=16 ef_construct=100 + binary quantization
- **Load:** 5.7 s (15,345 vectors/s); **index ready:** 1.0 s; **disk:** 452 MB; **RSS:** 2,817 MB after load, 595 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9960 | 0.9944 | 0.285 ms | 0.279 ms | 0.316 ms | 0.337 ms | 7,444 |
| 16 | 0.9870 | 0.9796 | 0.282 ms | 0.269 ms | 0.318 ms | 0.390 ms | 6,970 |
| 32 | 0.9910 | 0.9889 | 0.275 ms | 0.275 ms | 0.304 ms | 0.318 ms | 7,426 |
| 64 | 0.9970 | 0.9954 | 0.288 ms | 0.289 ms | 0.318 ms | 0.333 ms | 7,160 |
| 128 | 0.9990 | 0.9982 | 0.319 ms | 0.318 ms | 0.351 ms | 0.368 ms | 6,739 |
| 256 | 1.0000 | 0.9989 | 0.381 ms | 0.379 ms | 0.431 ms | 0.455 ms | 6,210 |
| 512 | 1.0000 | 0.9989 | 0.568 ms | 0.546 ms | 0.759 ms | 0.868 ms | 3,723 |

## HNSW + binary storage, int4 query (rescore, 2x oversampling)

- **Strategy:** HNSW m=16 ef_construct=100 + binary quantization
- **Load:** 5.6 s (15,711 vectors/s); **index ready:** 1.0 s; **disk:** 454 MB; **RSS:** 2,713 MB after load, 597 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9990 | 0.9964 | 0.293 ms | 0.286 ms | 0.334 ms | 0.364 ms | 7,529 |
| 16 | 0.9950 | 0.9865 | 0.295 ms | 0.276 ms | 0.363 ms | 0.831 ms | 7,204 |
| 32 | 0.9980 | 0.9932 | 0.275 ms | 0.275 ms | 0.304 ms | 0.324 ms | 7,310 |
| 64 | 1.0000 | 0.9977 | 0.288 ms | 0.292 ms | 0.327 ms | 0.348 ms | 7,470 |
| 128 | 1.0000 | 0.9989 | 0.323 ms | 0.323 ms | 0.359 ms | 0.377 ms | 6,872 |
| 256 | 1.0000 | 0.9997 | 0.389 ms | 0.387 ms | 0.436 ms | 0.466 ms | 6,060 |
| 512 | 1.0000 | 0.9998 | 0.572 ms | 0.529 ms | 0.815 ms | 0.936 ms | 3,902 |

## HNSW + binary storage, int8 query (rescore, 2x oversampling)

- **Strategy:** HNSW m=16 ef_construct=100 + binary quantization
- **Load:** 5.8 s (15,247 vectors/s); **index ready:** 1.0 s; **disk:** 453 MB; **RSS:** 2,810 MB after load, 594 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 1.0000 | 0.9971 | 0.301 ms | 0.296 ms | 0.342 ms | 0.370 ms | 7,341 |
| 16 | 0.9940 | 0.9862 | 0.299 ms | 0.280 ms | 0.344 ms | 0.513 ms | 7,601 |
| 32 | 0.9950 | 0.9932 | 0.280 ms | 0.280 ms | 0.309 ms | 0.321 ms | 7,207 |
| 64 | 1.0000 | 0.9978 | 0.299 ms | 0.299 ms | 0.330 ms | 0.348 ms | 7,158 |
| 128 | 1.0000 | 0.9997 | 0.338 ms | 0.339 ms | 0.374 ms | 0.396 ms | 6,836 |
| 256 | 1.0000 | 1.0000 | 0.414 ms | 0.412 ms | 0.464 ms | 0.495 ms | 5,745 |
| 512 | 1.0000 | 1.0000 | 0.652 ms | 0.625 ms | 0.874 ms | 1.013 ms | 4,094 |

## HNSW + product quantization x4 (rescore)

- **Strategy:** HNSW m=16 ef_construct=100 + product quantization
- **Load:** 4.2 s (20,842 vectors/s); **index ready:** 79.8 s; **disk:** 546 MB; **RSS:** 1,345 MB after load, 650 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9970 | 0.9908 | 1.325 ms | 1.285 ms | 1.517 ms | 1.932 ms | 1,799 |
| 16 | 0.9920 | 0.9730 | 1.028 ms | 1.017 ms | 1.132 ms | 1.233 ms | 2,196 |
| 32 | 0.9970 | 0.9870 | 1.143 ms | 1.136 ms | 1.265 ms | 1.368 ms | 1,897 |
| 64 | 0.9990 | 0.9929 | 1.362 ms | 1.357 ms | 1.504 ms | 1.577 ms | 1,791 |
| 128 | 0.9990 | 0.9955 | 1.762 ms | 1.746 ms | 2.025 ms | 2.234 ms | 1,432 |
| 256 | 1.0000 | 0.9963 | 2.456 ms | 2.458 ms | 2.930 ms | 3.056 ms | 1,118 |
| 512 | 1.0000 | 0.9964 | 3.788 ms | 3.841 ms | 4.585 ms | 4.891 ms | 868 |

## HNSW + product quantization x8 (rescore)

- **Strategy:** HNSW m=16 ef_construct=100 + product quantization
- **Load:** 4.4 s (20,216 vectors/s); **index ready:** 55.6 s; **disk:** 503 MB; **RSS:** 633 MB after load, 600 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9990 | 0.9896 | 0.836 ms | 0.826 ms | 0.927 ms | 0.995 ms | 2,987 |
| 16 | 0.9840 | 0.9673 | 0.703 ms | 0.701 ms | 0.773 ms | 0.823 ms | 3,318 |
| 32 | 0.9950 | 0.9852 | 0.770 ms | 0.766 ms | 0.844 ms | 0.899 ms | 3,262 |
| 64 | 0.9990 | 0.9906 | 0.866 ms | 0.842 ms | 0.981 ms | 1.320 ms | 2,925 |
| 128 | 1.0000 | 0.9918 | 1.051 ms | 1.047 ms | 1.164 ms | 1.222 ms | 2,380 |
| 256 | 1.0000 | 0.9920 | 1.466 ms | 1.392 ms | 1.630 ms | 4.539 ms | 1,832 |
| 512 | 1.0000 | 0.9921 | 2.010 ms | 2.013 ms | 2.372 ms | 2.525 ms | 1,366 |

## HNSW + product quantization x32 (rescore)

- **Strategy:** HNSW m=16 ef_construct=100 + product quantization
- **Load:** 4.3 s (20,399 vectors/s); **index ready:** 23.2 s; **disk:** 470 MB; **RSS:** 739 MB after load, 567 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9990 | 0.9874 | 0.446 ms | 0.439 ms | 0.506 ms | 0.576 ms | 5,816 |
| 16 | 0.9880 | 0.9709 | 0.391 ms | 0.389 ms | 0.446 ms | 0.491 ms | 6,620 |
| 32 | 0.9960 | 0.9843 | 0.377 ms | 0.374 ms | 0.421 ms | 0.465 ms | 6,281 |
| 64 | 0.9990 | 0.9880 | 0.403 ms | 0.399 ms | 0.457 ms | 0.504 ms | 6,047 |
| 128 | 1.0000 | 0.9891 | 0.506 ms | 0.500 ms | 0.574 ms | 0.650 ms | 4,757 |
| 256 | 1.0000 | 0.9903 | 0.640 ms | 0.612 ms | 0.756 ms | 0.957 ms | 4,024 |
| 512 | 1.0000 | 0.9904 | 0.775 ms | 0.773 ms | 0.895 ms | 0.974 ms | 3,352 |

## HNSW + product quantization x64 (rescore)

- **Strategy:** HNSW m=16 ef_construct=100 + product quantization
- **Load:** 4.4 s (20,020 vectors/s); **index ready:** 16.2 s; **disk:** 464 MB; **RSS:** 553 MB after load, 557 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9800 | 0.9136 | 0.391 ms | 0.381 ms | 0.444 ms | 0.496 ms | 6,874 |
| 16 | 0.9580 | 0.8899 | 0.320 ms | 0.318 ms | 0.356 ms | 0.386 ms | 7,208 |
| 32 | 0.9760 | 0.9088 | 0.331 ms | 0.328 ms | 0.382 ms | 0.431 ms | 6,826 |
| 64 | 0.9820 | 0.9155 | 0.381 ms | 0.356 ms | 0.459 ms | 0.771 ms | 6,561 |
| 128 | 0.9870 | 0.9191 | 0.386 ms | 0.382 ms | 0.442 ms | 0.485 ms | 6,205 |
| 256 | 0.9890 | 0.9202 | 0.486 ms | 0.482 ms | 0.571 ms | 0.644 ms | 5,237 |
| 512 | 0.9890 | 0.9208 | 0.641 ms | 0.631 ms | 0.757 ms | 0.856 ms | 3,742 |
