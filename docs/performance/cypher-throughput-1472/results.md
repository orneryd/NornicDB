# Paired local results

Medians of five alternating rounds. Negative latency changes are faster; small differences are noise, not established gains. Bytes and allocations are per operation.

| Parser | Workload | Control µs | Current µs | Latency change | B/op control → current | Allocs/op control → current |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| nornic | PipelineLimitedCartesianCreateDelete | 46.847 | 46.729 | -0.3% | 22359 → 22024 | 287 → 285 |
| nornic | PipelineWithScope/projection | 360.780 | 238.822 | -33.8% | 14493 → 14129 | 874 → 872 |
| nornic | PipelineWithScope/predicate | 334.213 | 518.745 | +55.2% | 15052 → 15171 | 894 → 894 |
| nornic | StatementRouting/autocommit/simple_match_limit | 30.634 | 30.469 | -0.5% | 11210 → 11239 | 138 → 138 |
| nornic | StatementRouting/explicit_tx/simple_match_limit | 15.733 | 21.072 | +33.9% | 5427 → 5433 | 80 → 80 |
| nornic | StatementRouting/autocommit/property_lookup | 1708.307 | 968.620 | -43.3% | 1452654 → 1452404 | 10200 → 10199 |
| nornic | StatementRouting/explicit_tx/property_lookup | 684.940 | 326.187 | -52.4% | 305037 → 300447 | 2232 → 2176 |
| nornic | StatementRouting/autocommit/filter_count | 1977.957 | 1020.966 | -48.4% | 1449982 → 1449367 | 10157 → 10155 |
| nornic | StatementRouting/explicit_tx/filter_count | 614.764 | 384.009 | -37.5% | 300723 → 297289 | 2158 → 2122 |
| nornic | StatementRouting/autocommit/one_hop_limit | 185.601 | 117.340 | -36.8% | 104145 → 103969 | 1255 → 1255 |
| nornic | StatementRouting/explicit_tx/one_hop_limit | 208.312 | 141.003 | -32.3% | 113053 → 112976 | 1423 → 1423 |
| nornic | StatementRouting/autocommit/return_literal | 8.903 | 7.812 | -12.3% | 2933 → 2931 | 39 → 39 |
| nornic | StatementRouting/explicit_tx/return_literal | 8.185 | 7.939 | -3.0% | 3061 → 3060 | 34 → 34 |
| nornic | SemanticValidationDistinctText | 30.342 | 34.566 | +13.9% | 3547 → 3523 | 43 → 43 |
| antlr | PipelineLimitedCartesianCreateDelete | 51.479 | 56.657 | +10.1% | 22490 → 22156 | 288 → 286 |
| antlr | PipelineWithScope/projection | 474.364 | 269.718 | -43.1% | 14723 → 14251 | 875 → 873 |
| antlr | PipelineWithScope/predicate | 595.655 | 594.580 | -0.2% | 15346 → 15347 | 895 → 895 |
| antlr | StatementRouting/autocommit/simple_match_limit | 35.661 | 30.752 | -13.8% | 11312 → 11305 | 139 → 139 |
| antlr | StatementRouting/explicit_tx/simple_match_limit | 24.899 | 24.410 | -2.0% | 5502 → 5501 | 81 → 81 |
| antlr | StatementRouting/autocommit/property_lookup | 2128.926 | 2092.999 | -1.7% | 1453281 → 1453353 | 10203 → 10203 |
| antlr | StatementRouting/explicit_tx/property_lookup | 695.654 | 706.398 | +1.5% | 305710 → 305284 | 2240 → 2235 |
| antlr | StatementRouting/autocommit/filter_count | 2333.957 | 2055.026 | -12.0% | 1450483 → 1450291 | 10160 → 10159 |
| antlr | StatementRouting/explicit_tx/filter_count | 654.156 | 642.510 | -1.8% | 302621 → 301346 | 2179 → 2165 |
| antlr | StatementRouting/autocommit/one_hop_limit | 192.509 | 177.058 | -8.0% | 104227 → 104230 | 1256 → 1256 |
| antlr | StatementRouting/explicit_tx/one_hop_limit | 215.206 | 185.966 | -13.6% | 113149 → 113109 | 1424 → 1424 |
| antlr | StatementRouting/autocommit/return_literal | 10.199 | 8.989 | -11.9% | 2967 → 2965 | 40 → 40 |
| antlr | StatementRouting/explicit_tx/return_literal | 8.151 | 7.350 | -9.8% | 3093 → 3092 | 35 → 35 |
| antlr | SemanticValidationDistinctText | 33.512 | 26.910 | -19.7% | 3528 → 3553 | 43 → 43 |
