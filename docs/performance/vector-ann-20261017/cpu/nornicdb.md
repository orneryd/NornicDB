# NornicDB: vector ANN benchmark

- **Corpus:** 88,155 vectors × 1024 dimensions (amazon.titan-embed-text-v2:0, cosine), chunks of NornicDB, code-intelligence, soraban-api, soraban-connect, soraban-react-app, atlas-backend, atlas-frontend, soraban-infra (4,362 documentation, 83,793 code); 1,000 held-out queries; exact top-100 ground truth.
- **Measured:** recall@10 against exact ground truth; sequential client latency over 1,000 queries after 100 warm-up; throughput with 8 concurrent RPCs on one shared channel.
- **Engines:** NornicDB v1.4.3 (`7c201376d1203a651fc625869fb98df7be746ac4`), Qdrant 1.19.2 (native binary); one server at a time, each on a fresh data directory, both driven over the Qdrant gRPC API (Upsert batches of 500, Search).
- **Machine:** Apple M2 Max, 12 cores, 32 GiB, Darwin 25.5.0.
- **Provenance:** effective settings and binary/corpus SHA-256 hashes are recorded in each result JSON; resolved dependencies are in `environment.json`.
- **NornicDB GPU paths:** see each run's effective settings in its JSON; CPU-only runs disable the GPU manager, including GPU clustering.

## Exact (brute force)

- **Strategy:** CPU brute-force
- **Load:** 12.6 s (6,970 vectors/s); **index ready:** 4.2 s; **disk:** 1,213 MB; **RSS:** 3,749 MB after load, 3,749 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 1.0000 | 1.0000 | 6.381 ms | 6.308 ms | 6.579 ms | 7.793 ms | 186 |

## HNSW fast (M=16, efConstruction=100, efSearch=50)

- **Strategy:** HNSW
- **Load:** 12.8 s (6,872 vectors/s); **index ready:** 49.4 s; **disk:** 1,238 MB; **RSS:** 3,723 MB after load, 3,723 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9970 | 0.9947 | 0.817 ms | 0.811 ms | 0.986 ms | 1.077 ms | 6,322 |
| 16 | 0.9840 | 0.9705 | 0.490 ms | 0.486 ms | 0.586 ms | 0.641 ms | 8,920 |
| 32 | 0.9950 | 0.9909 | 0.635 ms | 0.631 ms | 0.771 ms | 0.855 ms | 7,919 |
| 64 | 0.9970 | 0.9958 | 0.907 ms | 0.902 ms | 1.100 ms | 1.181 ms | 4,594 |
| 128 | 1.0000 | 0.9988 | 1.378 ms | 1.370 ms | 1.700 ms | 1.888 ms | 3,691 |
| 256 | 1.0000 | 0.9998 | 2.250 ms | 2.231 ms | 2.859 ms | 3.192 ms | 2,214 |
| 512 | 1.0000 | 1.0000 | 3.858 ms | 3.811 ms | 4.976 ms | 5.446 ms | 1,274 |

## HNSW balanced (M=16, efConstruction=200, efSearch=100)

- **Strategy:** HNSW
- **Load:** 12.7 s (6,930 vectors/s); **index ready:** 71.1 s; **disk:** 1,239 MB; **RSS:** 3,746 MB after load, 3,746 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9990 | 0.9992 | 1.224 ms | 1.207 ms | 1.522 ms | 1.658 ms | 4,046 |
| 16 | 0.9870 | 0.9728 | 0.498 ms | 0.490 ms | 0.590 ms | 0.663 ms | 8,615 |
| 32 | 0.9980 | 0.9927 | 0.680 ms | 0.666 ms | 0.843 ms | 1.007 ms | 7,612 |
| 64 | 0.9990 | 0.9984 | 0.940 ms | 0.936 ms | 1.133 ms | 1.228 ms | 5,198 |
| 128 | 0.9990 | 0.9992 | 1.425 ms | 1.416 ms | 1.762 ms | 1.923 ms | 3,252 |
| 256 | 1.0000 | 0.9998 | 2.270 ms | 2.252 ms | 2.828 ms | 3.213 ms | 1,976 |
| 512 | 1.0000 | 1.0000 | 3.791 ms | 3.777 ms | 4.649 ms | 5.305 ms | 1,137 |

## HNSW accurate (M=32, efConstruction=400, efSearch=200)

- **Strategy:** HNSW
- **Load:** 12.7 s (6,957 vectors/s); **index ready:** 144.8 s; **disk:** 1,238 MB; **RSS:** 3,760 MB after load, 3,760 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 1.0000 | 0.9997 | 2.813 ms | 2.803 ms | 3.532 ms | 3.951 ms | 1,685 |
| 16 | 0.9970 | 0.9837 | 0.619 ms | 0.612 ms | 0.754 ms | 0.846 ms | 7,388 |
| 32 | 0.9990 | 0.9965 | 0.864 ms | 0.857 ms | 1.047 ms | 1.156 ms | 5,564 |
| 64 | 1.0000 | 0.9991 | 1.292 ms | 1.291 ms | 1.598 ms | 1.761 ms | 3,840 |
| 128 | 1.0000 | 0.9994 | 2.134 ms | 2.088 ms | 2.670 ms | 4.377 ms | 2,258 |
| 256 | 1.0000 | 0.9999 | 3.428 ms | 3.395 ms | 4.446 ms | 4.815 ms | 1,360 |
| 512 | 1.0000 | 1.0000 | 5.656 ms | 5.662 ms | 7.103 ms | 7.901 ms | 813 |

## IVF (k-means routing, IVF-HNSW)

- **Strategy:** IVF-HNSW (per-cluster HNSW)
- **Load:** 13.0 s (6,770 vectors/s); **index ready:** 54.4 s; **disk:** 1,238 MB; **RSS:** 3,494 MB after load, 3,877 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9960 | 0.9741 | 14.163 ms | 18.177 ms | 21.607 ms | 22.906 ms | 510 |

## IVF-PQ (compressed)

- **Strategy:** IVFPQ compressed (lists=209 segments=128 bits=8 nprobe=27) [vectors=88155 sample=88155 lists=209 avg_list=421.8 max_list=1244 bytes_per_vector=153.79 duration=1m42.6493685s]
- **Load:** 12.9 s (6,815 vectors/s); **index ready:** 106.9 s; **disk:** 1,651 MB; **RSS:** 3,893 MB after load, 3,894 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9920 | 0.9773 | 5.211 ms | 5.175 ms | 5.707 ms | 6.214 ms | 955 |

## HNSW + scalar int8 quantization (rescore)

- **Strategy:** HNSW scalar int8 (90623340 encoded bytes)
- **Load:** 13.0 s (6,804 vectors/s); **index ready:** 36.8 s; **disk:** 1,631 MB; **RSS:** 3,221 MB after load, 3,221 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9960 | 0.9959 | 0.565 ms | 0.551 ms | 0.669 ms | 0.732 ms | 7,998 |
| 16 | 0.9840 | 0.9725 | 0.430 ms | 0.412 ms | 0.534 ms | 0.636 ms | 7,818 |
| 32 | 0.9960 | 0.9931 | 0.489 ms | 0.475 ms | 0.608 ms | 0.753 ms | 8,935 |
| 64 | 0.9970 | 0.9972 | 0.595 ms | 0.583 ms | 0.725 ms | 0.834 ms | 8,270 |
| 128 | 1.0000 | 0.9992 | 0.791 ms | 0.786 ms | 0.932 ms | 1.008 ms | 6,314 |
| 256 | 1.0000 | 0.9998 | 1.145 ms | 1.139 ms | 1.377 ms | 1.501 ms | 5,513 |
| 512 | 1.0000 | 1.0000 | 1.759 ms | 1.751 ms | 2.119 ms | 2.341 ms | 3,743 |

## HNSW + product quantization x16 (rescore)

- **Strategy:** HNSW PQ x16 (23616256 encoded bytes)
- **Load:** 13.2 s (6,679 vectors/s); **index ready:** 40.5 s; **disk:** 1,632 MB; **RSS:** 3,180 MB after load, 3,180 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9950 | 0.9892 | 0.923 ms | 0.904 ms | 1.077 ms | 1.154 ms | 6,030 |
| 16 | 0.9850 | 0.9700 | 0.736 ms | 0.734 ms | 0.879 ms | 0.934 ms | 7,765 |
| 32 | 0.9950 | 0.9859 | 0.819 ms | 0.810 ms | 0.978 ms | 1.050 ms | 7,662 |
| 64 | 0.9970 | 0.9914 | 0.953 ms | 0.939 ms | 1.124 ms | 1.242 ms | 6,767 |
| 128 | 1.0000 | 0.9928 | 1.229 ms | 1.220 ms | 1.454 ms | 1.539 ms | 5,230 |
| 256 | 1.0000 | 0.9932 | 1.648 ms | 1.636 ms | 1.995 ms | 2.136 ms | 4,178 |
| 512 | 1.0000 | 0.9936 | 2.392 ms | 2.375 ms | 2.899 ms | 3.155 ms | 3,039 |

## HNSW + binary quantization (rescore, 2x oversampling)

- **Strategy:** HNSW binary (11283840 encoded bytes)
- **Load:** 13.0 s (6,758 vectors/s); **index ready:** 35.4 s; **disk:** 1,629 MB; **RSS:** 3,180 MB after load, 3,181 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9960 | 0.9803 | 0.610 ms | 0.575 ms | 0.785 ms | 1.434 ms | 7,511 |
| 16 | 0.9910 | 0.9732 | 0.533 ms | 0.517 ms | 0.665 ms | 0.956 ms | 7,603 |
| 32 | 0.9920 | 0.9741 | 0.499 ms | 0.492 ms | 0.620 ms | 0.687 ms | 7,010 |
| 64 | 0.9960 | 0.9805 | 0.561 ms | 0.552 ms | 0.699 ms | 0.770 ms | 8,778 |
| 128 | 1.0000 | 0.9822 | 0.656 ms | 0.647 ms | 0.801 ms | 0.900 ms | 7,560 |
| 256 | 1.0000 | 0.9822 | 0.785 ms | 0.768 ms | 0.954 ms | 1.052 ms | 7,100 |
| 512 | 1.0000 | 0.9828 | 0.982 ms | 0.974 ms | 1.158 ms | 1.214 ms | 5,774 |

## HNSW + binary storage, int4 query (rescore, 2x oversampling)

- **Strategy:** HNSW binary query int4 (11283840 encoded bytes)
- **Load:** 13.1 s (6,744 vectors/s); **index ready:** 35.9 s; **disk:** 1,629 MB; **RSS:** 3,179 MB after load, 3,180 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9960 | 0.9925 | 0.658 ms | 0.644 ms | 0.805 ms | 0.905 ms | 5,424 |
| 16 | 0.9930 | 0.9884 | 0.573 ms | 0.563 ms | 0.704 ms | 0.791 ms | 8,569 |
| 32 | 0.9940 | 0.9886 | 0.599 ms | 0.579 ms | 0.738 ms | 0.806 ms | 8,199 |
| 64 | 0.9960 | 0.9937 | 0.658 ms | 0.646 ms | 0.806 ms | 0.954 ms | 7,846 |
| 128 | 0.9990 | 0.9952 | 0.744 ms | 0.737 ms | 0.902 ms | 0.995 ms | 6,901 |
| 256 | 1.0000 | 0.9955 | 0.903 ms | 0.891 ms | 1.104 ms | 1.225 ms | 5,961 |
| 512 | 1.0000 | 0.9959 | 1.240 ms | 1.235 ms | 1.478 ms | 1.604 ms | 4,886 |

## HNSW + binary storage, int8 query (rescore, 2x oversampling)

- **Strategy:** HNSW binary query int8 (11283840 encoded bytes)
- **Load:** 12.9 s (6,816 vectors/s); **index ready:** 35.6 s; **disk:** 1,629 MB; **RSS:** 3,221 MB after load, 3,221 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9960 | 0.9939 | 0.697 ms | 0.676 ms | 0.849 ms | 0.933 ms | 6,697 |
| 16 | 0.9930 | 0.9879 | 0.624 ms | 0.614 ms | 0.765 ms | 0.830 ms | 7,391 |
| 32 | 0.9930 | 0.9887 | 0.628 ms | 0.622 ms | 0.766 ms | 0.828 ms | 8,432 |
| 64 | 0.9960 | 0.9948 | 0.744 ms | 0.729 ms | 0.889 ms | 0.985 ms | 6,560 |
| 128 | 0.9990 | 0.9967 | 0.874 ms | 0.862 ms | 1.044 ms | 1.147 ms | 6,898 |
| 256 | 1.0000 | 0.9969 | 1.181 ms | 1.110 ms | 1.384 ms | 2.762 ms | 5,494 |
| 512 | 1.0000 | 0.9971 | 1.531 ms | 1.508 ms | 1.893 ms | 2.062 ms | 4,219 |

## HNSW + product quantization x4 (rescore)

- **Strategy:** HNSW PQ x4 (91319296 encoded bytes)
- **Load:** 13.1 s (6,726 vectors/s); **index ready:** 44.7 s; **disk:** 1,631 MB; **RSS:** 3,202 MB after load, 3,202 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9960 | 0.9958 | 1.794 ms | 1.757 ms | 2.135 ms | 2.434 ms | 3,902 |
| 16 | 0.9850 | 0.9736 | 1.314 ms | 1.299 ms | 1.611 ms | 1.718 ms | 4,983 |
| 32 | 0.9960 | 0.9929 | 1.570 ms | 1.552 ms | 1.901 ms | 2.069 ms | 4,108 |
| 64 | 0.9970 | 0.9972 | 1.966 ms | 1.911 ms | 2.328 ms | 4.083 ms | 3,569 |
| 128 | 1.0000 | 0.9992 | 2.770 ms | 2.761 ms | 3.272 ms | 3.552 ms | 2,598 |
| 256 | 1.0000 | 0.9998 | 4.238 ms | 4.249 ms | 5.129 ms | 5.402 ms | 1,679 |
| 512 | 1.0000 | 1.0000 | 6.875 ms | 6.904 ms | 8.326 ms | 8.929 ms | 1,105 |

## HNSW + product quantization x8 (rescore)

- **Strategy:** HNSW PQ x8 (46183936 encoded bytes)
- **Load:** 12.9 s (6,809 vectors/s); **index ready:** 42.5 s; **disk:** 1,630 MB; **RSS:** 3,204 MB after load, 3,204 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9950 | 0.9947 | 1.258 ms | 1.221 ms | 1.492 ms | 1.661 ms | 5,557 |
| 16 | 0.9840 | 0.9726 | 0.939 ms | 0.934 ms | 1.100 ms | 1.190 ms | 6,471 |
| 32 | 0.9950 | 0.9920 | 1.056 ms | 1.039 ms | 1.256 ms | 1.336 ms | 6,144 |
| 64 | 0.9960 | 0.9968 | 1.414 ms | 1.379 ms | 1.670 ms | 2.627 ms | 5,162 |
| 128 | 1.0000 | 0.9990 | 1.748 ms | 1.728 ms | 2.105 ms | 2.339 ms | 4,110 |
| 256 | 1.0000 | 0.9994 | 2.482 ms | 2.485 ms | 2.941 ms | 3.091 ms | 2,776 |
| 512 | 1.0000 | 0.9996 | 3.901 ms | 3.912 ms | 4.671 ms | 4.943 ms | 1,859 |

## HNSW + product quantization x32 (rescore)

- **Strategy:** HNSW PQ x32 (12332416 encoded bytes)
- **Load:** 13.0 s (6,757 vectors/s); **index ready:** 38.8 s; **disk:** 1,630 MB; **RSS:** 3,169 MB after load, 3,169 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9960 | 0.9528 | 0.694 ms | 0.683 ms | 0.822 ms | 0.892 ms | 6,429 |
| 16 | 0.9810 | 0.9378 | 0.580 ms | 0.571 ms | 0.698 ms | 0.766 ms | 8,100 |
| 32 | 0.9930 | 0.9508 | 0.629 ms | 0.620 ms | 0.758 ms | 0.806 ms | 8,579 |
| 64 | 0.9970 | 0.9539 | 0.721 ms | 0.699 ms | 0.835 ms | 0.918 ms | 4,859 |
| 128 | 1.0000 | 0.9553 | 0.866 ms | 0.856 ms | 1.034 ms | 1.152 ms | 7,253 |
| 256 | 1.0000 | 0.9556 | 1.131 ms | 1.123 ms | 1.344 ms | 1.466 ms | 5,691 |
| 512 | 1.0000 | 0.9555 | 1.572 ms | 1.554 ms | 1.905 ms | 2.036 ms | 4,144 |

## HNSW + product quantization x64 (rescore)

- **Strategy:** HNSW PQ x64 (6690496 encoded bytes)
- **Load:** 13.0 s (6,805 vectors/s); **index ready:** 38.4 s; **disk:** 1,630 MB; **RSS:** 3,222 MB after load, 3,222 MB after queries

| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| preset beam | 0.9800 | 0.8571 | 0.613 ms | 0.595 ms | 0.730 ms | 0.808 ms | 8,799 |
| 16 | 0.9560 | 0.8435 | 0.541 ms | 0.515 ms | 0.647 ms | 0.746 ms | 8,474 |
| 32 | 0.9720 | 0.8535 | 0.555 ms | 0.542 ms | 0.674 ms | 0.747 ms | 7,311 |
| 64 | 0.9810 | 0.8576 | 0.642 ms | 0.620 ms | 0.762 ms | 1.097 ms | 8,331 |
| 128 | 0.9840 | 0.8590 | 0.692 ms | 0.679 ms | 0.827 ms | 0.920 ms | 6,909 |
| 256 | 0.9850 | 0.8592 | 0.857 ms | 0.844 ms | 1.009 ms | 1.102 ms | 7,456 |
| 512 | 0.9850 | 0.8595 | 1.138 ms | 1.128 ms | 1.362 ms | 1.472 ms | 5,267 |
