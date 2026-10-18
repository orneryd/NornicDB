## Results

Median of five runs. Latencies are milliseconds per operation; build rows are seconds.

| Workload | Develop | Original PR | Tuned | Tuned vs original |
| --- | ---: | ---: | ---: | ---: |
| HNSWChurn/remove_reinsert | 0.126 | 0.134 | 0.128 | -4.4% |
| HNSWChurn/load_after_churn | 7.454 | 7.525 | 7.547 | +0.3% |
| HNSWChurn/recall_near_removed_region | 0.009 (0.084 recall) | 0.089 (0.831 recall) | 0.090 (0.831 recall) | +1.0% |
| HNSWChurn/search_after_churn | 0.087 (0.869 recall) | 0.092 (0.941 recall) | 0.094 (0.941 recall) | +2.3% |
| HNSWChurn/search_near_removed_region | 0.003 | 0.080 | 0.082 | +1.6% |
| HNSWIndex_Add | 0.413 | 0.409 | 0.405 | -1.1% |
| HNSWIndex_Search | 0.140 | 0.141 | 0.141 | +0.0% |
| HNSWSearch2048 | 1.227 | 1.259 | 1.233 | -2.0% |
| HNSWBuild2048/n2000/identicalfalse/lexicalfalse | 1.884 | 1.882 | 1.956 | +3.9% |
| HNSWBuild2048/n2000/identicalfalse/lexicaltrue | 1.894 | 1.895 | 1.923 | +1.5% |
| HNSWBuild2048/n2000/identicaltrue/lexicalfalse | 1.403 | 1.388 | 1.388 | +0.0% |
| HNSWBuild2048/n2000/identicaltrue/lexicaltrue | 5.259 | 5.296 | 5.248 | -0.9% |
| HNSWBuild2048/n5300/identicalfalse/lexicalfalse | 10.434 | 10.573 | 11.039 | +4.4% |
| HNSWBuild2048/n5300/identicalfalse/lexicaltrue | 10.765 | 10.809 | 10.415 | -3.6% |
| HNSWBuild2048/n5300/identicaltrue/lexicalfalse | 3.797 | 3.754 | 3.726 | -0.8% |
| HNSWBuild2048/n5300/identicaltrue/lexicaltrue | 14.370 | 14.561 | 14.566 | +0.0% |
| SearchTextContinuationRankedHNSW/cache=false/all | 10.083 (2000 docs) | 9.291 (2000 docs) | 9.156 (2000 docs) | -1.5% |
| SearchTextContinuationRankedHNSW/cache=false/selective | 0.557 (11 docs) | 0.499 (11 docs) | 0.501 (11 docs) | +0.3% |
| SearchTextContinuationRankedHNSW/cache=true/all | 1.982 (2000 docs) | 1.800 (2000 docs) | 1.757 (2000 docs) | -2.4% |
| SearchTextContinuationRankedHNSW/cache=true/selective | 0.014 (11 docs) | 0.012 (11 docs) | 0.012 (11 docs) | -3.0% |
| SearchTextContinuationRankedIVFHNSW/cache=false/all | 0.532 (150 docs) | 4.978 (2000 docs) | 4.839 (2000 docs) | -2.8% |
| SearchTextContinuationRankedIVFHNSW/cache=false/selective | 0.049 (11 docs) | 0.268 (11 docs) | 0.259 (11 docs) | -3.3% |
| SearchTextContinuationRankedIVFHNSW/cache=true/all | 0.111 (150 docs) | 1.866 (2000 docs) | 1.829 (2000 docs) | -2.0% |
| SearchTextContinuationRankedIVFHNSW/cache=true/selective | 0.008 (11 docs) | 0.012 (11 docs) | 0.012 (11 docs) | -3.0% |
| IVFPQCandidateGen | 0.038 | 0.039 | 0.038 | -1.7% |
| IVFPQCandidateGenWithOverlay | 0.000 | 0.000 | 0.000 | -1.3% |
