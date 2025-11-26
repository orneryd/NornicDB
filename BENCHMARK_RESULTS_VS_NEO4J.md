stdout | testing\benchmarks\nornicdb-vs-neo4j.bench.ts:326:11

╔════════════════════════════════════════════════════════════════════╗

stdout | testing\benchmarks\nornicdb-vs-neo4j.bench.ts:327:11
║         NornicDB vs Neo4j Performance Benchmark Suite              ║

stdout | testing\benchmarks\nornicdb-vs-neo4j.bench.ts:328:11
╚════════════════════════════════════════════════════════════════════╝


stdout | testing\benchmarks\nornicdb-vs-neo4j.bench.ts:331:11
Connecting to NornicDB at bolt://localhost:7687...

stdout | testing\benchmarks\nornicdb-vs-neo4j.bench.ts:336:13
✓ Connected to NornicDB

stdout | testing\benchmarks\nornicdb-vs-neo4j.bench.ts:338:13
Loading Movies dataset into NornicDB...

stdout | testing\benchmarks\nornicdb-vs-neo4j.bench.ts:341:13
  → 40 nodes created in NornicDB

stdout | testing\benchmarks\nornicdb-vs-neo4j.bench.ts:347:11

Connecting to Neo4j at bolt://localhost:7688...

stdout | testing\benchmarks\nornicdb-vs-neo4j.bench.ts:352:13
✓ Connected to Neo4j

stdout | testing\benchmarks\nornicdb-vs-neo4j.bench.ts:354:13
Loading Movies dataset into Neo4j...

stdout | testing\benchmarks\nornicdb-vs-neo4j.bench.ts:357:13
  → 40 nodes created in Neo4j

stdout | testing\benchmarks\nornicdb-vs-neo4j.bench.ts:362:11

────────────────────────────────────────────────────────────────────────



 ✓ testing/benchmarks/nornicdb-vs-neo4j.bench.ts > NornicDB Benchmarks 11053ms
     name                                   hz      min      max     mean      p75      p99     p995     p999     rme  samples
   · Count all nodes                    147.76   6.1990   7.8490   6.7676   7.0787   7.8490   7.8490   7.8490  ±1.26%       74
   · Count all relationships           45.5454  20.8197  24.4062  21.9561  22.3362  24.4062  24.4062  24.4062  ±1.60%       23
   · Get all movies                     608.79   1.2878   5.4866   1.6426   1.7132   2.8817   3.7663   5.4866  ±2.53%      305
   · Get all people                     414.88   1.8739   6.0335   2.4103   2.5270   3.4863   3.8334   6.0335  ±2.44%      208
   · Find movie by title                603.42   1.2750   9.6298   1.6572   1.6956   2.8473   6.8458   9.6298  ±4.17%      302
   · Find person by name                467.61   1.8612   8.7531   2.1385   2.1686   2.9615   3.2988   8.7531  ±2.81%      234
   · Actors in The Matrix               649.29   1.2436   3.9641   1.5401   1.5774   1.8768   2.0626   3.9641  ±1.21%      325
   · Movies Keanu acted in              508.74   1.6844   3.5380   1.9656   2.0063   3.2103   3.3020   3.5380  ±1.52%      255
   · Co-actors of Keanu                 510.24   1.6699   2.3309   1.9599   1.9942   2.2870   2.3069   2.3309  ±0.56%      256
   · Directors of co-actors movies      499.58   1.7604   3.0665   2.0017   2.0174   2.8130   2.8430   3.0665  ±1.10%      250
   · Movies per decade                  786.33   1.0226   1.9904   1.2717   1.3109   1.6514   1.8525   1.9904  ±0.83%      394
   · Most prolific actors               133.34   6.9411   8.6935   7.4994   7.7611   8.6935   8.6935   8.6935  ±1.31%       67
   · Actor-Director pairs               132.63   7.0414  11.1892   7.5400   7.6997  11.1892  11.1892  11.1892  ±1.98%       67
   · Movies with or without directors   167.69   5.5729   7.2469   5.9635   6.0110   7.2469   7.2469   7.2469  ±1.14%       85
   · Top movies by actor count          953.15   0.8639   2.4138   1.0492   1.0756   1.3245   1.4371   2.4138  ±0.83%      477
   · Movies with cast list              156.99   5.9617   7.8651   6.3700   6.5266   7.8651   7.8651   7.8651  ±1.25%       79
   · Create and delete node             369.47   1.8014   5.4763   2.7066   3.1235   4.1541   5.4763   5.4763  ±3.24%      185
   · Create and delete relationship     435.57   2.1007   3.0768   2.2958   2.3138   2.9438   2.9997   3.0768  ±0.96%      218
stdout | testing\benchmarks\nornicdb-vs-neo4j.bench.ts:366:11

────────────────────────────────────────────────────────────────────────

stdout | testing\benchmarks\nornicdb-vs-neo4j.bench.ts:367:11
Cleaning up...

stdout | testing\benchmarks\nornicdb-vs-neo4j.bench.ts:381:11
✓ Cleanup complete



 ✓ testing/benchmarks/nornicdb-vs-neo4j.bench.ts > Neo4j Benchmarks 11119ms
     name                                  hz     min      max    mean     p75     p99    p995     p999     rme  samples
   · Count all nodes                   300.50  2.3078   9.4865  3.3278  3.4982  6.0967  9.4865   9.4865  ±3.68%      151
   · Count all relationships           373.62  2.0273   5.6052  2.6765  2.8610  5.3693  5.6052   5.6052  ±2.67%      187
   · Get all movies                    353.16  1.8667   5.1994  2.8316  3.0623  4.3920  5.1994   5.1994  ±2.45%      177
   · Get all people                    502.23  1.4762   6.3643  1.9911  2.1769  3.3047  3.5538   6.3643  ±2.64%      252
   · Find movie by title               568.08  1.4533   2.7716  1.7603  1.8521  2.6932  2.7111   2.7716  ±1.51%      285
   · Find person by name               565.25  1.3534   3.3581  1.7691  1.9182  2.8394  2.9055   3.3581  ±2.06%      283
   · Actors in The Matrix              544.83  1.4309   5.4325  1.8354  1.9363  2.4809  2.5744   5.4325  ±1.91%      273
   · Movies Keanu acted in             615.20  1.2520   2.7439  1.6255  1.7162  2.4349  2.6081   2.7439  ±1.50%      308
   · Co-actors of Keanu                582.26  1.2830  11.7937  1.7174  1.7738  2.6817  5.9460  11.7937  ±4.49%      292
   · Directors of co-actors movies     524.65  1.4189   8.4705  1.9060  2.0483  2.9469  3.0075   8.4705  ±3.14%      263
   · Movies per decade                 602.37  1.2104   5.3181  1.6601  1.7331  2.8000  3.5340   5.3181  ±2.43%      302
   · Most prolific actors              561.22  1.3682   2.9126  1.7818  1.9164  2.6529  2.7649   2.9126  ±1.57%      281
   · Actor-Director pairs              503.18  1.3236   3.4069  1.9874  2.1654  3.0983  3.2740   3.4069  ±2.30%      252
   · Movies with or without directors  569.70  1.1284   2.7386  1.7553  1.9049  2.3996  2.4011   2.7386  ±1.58%      285
   · Top movies by actor count         627.31  1.1996   2.8247  1.5941  1.6676  2.4812  2.6017   2.8247  ±1.51%      314
   · Movies with cast list             643.36  1.2805   2.5156  1.5543  1.6181  2.2826  2.4369   2.5156  ±1.41%      322
   · Create and delete node            445.71  1.4262   4.9219  2.2436  2.5786  3.5690  3.6973   4.9219  ±2.92%      223
   · Create and delete relationship    481.45  1.5307   5.3954  2.0770  2.2597  3.9057  4.6420   5.3954  ±3.04%      241

 BENCH  Summary

  Top movies by actor count - testing/benchmarks/nornicdb-vs-neo4j.bench.ts > NornicDB Benchmarks
    1.21x faster than Movies per decade
    1.47x faster than Actors in The Matrix
    1.57x faster than Get all movies
    1.58x faster than Find movie by title
    1.87x faster than Co-actors of Keanu
    1.87x faster than Movies Keanu acted in
    1.91x faster than Directors of co-actors movies
    2.04x faster than Find person by name
    2.19x faster than Create and delete relationship
    2.30x faster than Get all people
    2.58x faster than Create and delete node
    5.68x faster than Movies with or without directors
    6.07x faster than Movies with cast list
    6.45x faster than Count all nodes
    7.15x faster than Most prolific actors
    7.19x faster than Actor-Director pairs
    20.93x faster than Count all relationships
    1.21x faster than Movies per decade
    1.47x faster than Actors in The Matrix
    1.57x faster than Get all movies
    1.58x faster than Find movie by title
    1.87x faster than Co-actors of Keanu
    1.87x faster than Movies Keanu acted in
    1.91x faster than Directors of co-actors movies
    2.04x faster than Find person by name
    2.19x faster than Create and delete relationship
    2.30x faster than Get all people
    2.58x faster than Create and delete node
    5.68x faster than Movies with or without directors
    6.07x faster than Movies with cast list
    6.45x faster than Count all nodes
    7.15x faster than Most prolific actors
    7.19x faster than Actor-Director pairs
    20.93x faster than Count all relationships
    1.58x faster than Find movie by title
    1.87x faster than Co-actors of Keanu
    1.87x faster than Movies Keanu acted in
    1.91x faster than Directors of co-actors movies
    2.04x faster than Find person by name
    2.19x faster than Create and delete relationship
    2.30x faster than Get all people
    2.58x faster than Create and delete node
    5.68x faster than Movies with or without directors
    6.07x faster than Movies with cast list
    6.45x faster than Count all nodes
    7.15x faster than Most prolific actors
    7.19x faster than Actor-Director pairs
    20.93x faster than Count all relationships
    2.04x faster than Find person by name
    2.19x faster than Create and delete relationship
    2.30x faster than Get all people
    2.58x faster than Create and delete node
    5.68x faster than Movies with or without directors
    6.07x faster than Movies with cast list
    6.45x faster than Count all nodes
    7.15x faster than Most prolific actors
    7.19x faster than Actor-Director pairs
    20.93x faster than Count all relationships
    5.68x faster than Movies with or without directors
    6.07x faster than Movies with cast list
    6.45x faster than Count all nodes
    7.15x faster than Most prolific actors
    7.19x faster than Actor-Director pairs
    20.93x faster than Count all relationships
    7.15x faster than Most prolific actors
    7.19x faster than Actor-Director pairs
    20.93x faster than Count all relationships

  Movies with cast list - testing/benchmarks/nornicdb-vs-neo4j.bench.ts > Neo4j Benchmarks
    1.03x faster than Top movies by actor count
    1.05x faster than Movies Keanu acted in

  Movies with cast list - testing/benchmarks/nornicdb-vs-neo4j.bench.ts > Neo4j Benchmarks
    1.03x faster than Top movies by actor count
    1.05x faster than Movies Keanu acted in
    1.03x faster than Top movies by actor count
    1.05x faster than Movies Keanu acted in
    1.07x faster than Movies per decade
    1.10x faster than Co-actors of Keanu
    1.13x faster than Movies with or without directors
    1.13x faster than Find movie by title
    1.07x faster than Movies per decade
    1.10x faster than Co-actors of Keanu
    1.13x faster than Movies with or without directors
    1.13x faster than Find movie by title
    1.13x faster than Find movie by title
    1.14x faster than Find person by name
    1.15x faster than Most prolific actors
    1.18x faster than Actors in The Matrix
    1.23x faster than Directors of co-actors movies
    1.28x faster than Actor-Director pairs
    1.28x faster than Get all people
    1.34x faster than Create and delete relationship
    1.44x faster than Create and delete node
    1.72x faster than Count all relationships
    1.82x faster than Get all movies
    2.14x faster than Count all nodes


 RUN  v3.2.4 C:/Users/timot/Documents/GitHub/Mimir

stdout | testing\benchmarks\nornicdb-vs-neo4j-northwind.bench.ts:267:11

╔════════════════════════════════════════════════════════════════════╗

stdout | testing\benchmarks\nornicdb-vs-neo4j-northwind.bench.ts:268:11
║      NornicDB vs Neo4j - Northwind Dataset Benchmarks             ║

stdout | testing\benchmarks\nornicdb-vs-neo4j-northwind.bench.ts:269:11
╚════════════════════════════════════════════════════════════════════╝


stdout | testing\benchmarks\nornicdb-vs-neo4j-northwind.bench.ts:272:11
Connecting to NornicDB at bolt://localhost:7687...

stdout | testing\benchmarks\nornicdb-vs-neo4j-northwind.bench.ts:277:13
✓ Connected to NornicDB

stdout | testing\benchmarks\nornicdb-vs-neo4j-northwind.bench.ts:279:13
Loading Northwind dataset into NornicDB...

stdout | testing\benchmarks\nornicdb-vs-neo4j-northwind.bench.ts:282:13
  → 48 nodes created in NornicDB

stdout | testing\benchmarks\nornicdb-vs-neo4j-northwind.bench.ts:284:13
  → 1 relationships created in NornicDB

stdout | testing\benchmarks\nornicdb-vs-neo4j-northwind.bench.ts:290:11

Connecting to Neo4j at bolt://localhost:7688...

stdout | testing\benchmarks\nornicdb-vs-neo4j-northwind.bench.ts:295:13
✓ Connected to Neo4j

stdout | testing\benchmarks\nornicdb-vs-neo4j-northwind.bench.ts:297:13
Loading Northwind dataset into Neo4j...

stderr | testing\benchmarks\nornicdb-vs-neo4j-northwind.bench.ts:304:13
✗ Failed to connect to Neo4j: Neo4jError: WITH is required between CREATE and MATCH (line 7, column 5 (offset: 243))
"    MATCH (s1:Supplier {supplierID: 1}), (c1:Category {categoryID: 1})"
     ^

stdout | testing\benchmarks\nornicdb-vs-neo4j-northwind.bench.ts:307:11

────────────────────────────────────────────────────────────────────────


stdout | testing\benchmarks\nornicdb-vs-neo4j-northwind.bench.ts:311:11

────────────────────────────────────────────────────────────────────────

stdout | testing\benchmarks\nornicdb-vs-neo4j-northwind.bench.ts:312:11
Cleaning up...


 ✓ testing/benchmarks/nornicdb-vs-neo4j-northwind.bench.ts > Neo4j Benchmarks (Northwind) 17929ms
     name                                                  hz     min      max    mean     p75     p99    p995     p999     rme  samples
   · Count all nodes                                   251.95  2.4804   9.0180  3.9691  4.2084  7.5345  9.0180   9.0180  ±3.65%      126
   · Count all relationships                           332.38  2.0977   5.4392  3.0086  3.3614  5.4219  5.4392   5.4392  ±2.92%      167
   · Get all products                                  360.81  2.0362   7.7028  2.7716  2.9844  5.1473  7.7028   7.7028  ±2.96%      181
   · Get all categories                                325.74  2.0689   9.6725  3.0699  3.2818  9.3122  9.6725   9.6725  ±4.75%      163
   · Get all customers                                 459.96  1.5631   3.5589  2.1741  2.3370  3.4298  3.4537   3.5589  ±2.00%      230
   · Find product by name                              523.81  1.4415   4.5116  1.9091  2.0221  2.8271  2.9122   4.5116  ±1.95%      263
   · Find category by name                             476.12  1.6132   3.1301  2.1003  2.2453  2.8782  2.8814   3.1301  ±1.58%      239
   · Find customer by ID                               527.88  1.4798   3.0608  1.8944  2.0351  2.7672  2.9147   3.0608  ±1.55%      264
   · Products in Beverages category                    510.61  1.1224   6.2624  1.9585  2.1587  4.6798  4.9600   6.2624  ±4.19%      256
   · Products supplied by Exotic Liquids               604.89  1.3289   7.7029  1.6532  1.7320  2.1194  2.3905   7.7029  ±2.63%      303
   · Orders by customer ALFKI                          579.86  1.3155   2.6344  1.7246  1.8349  2.5374  2.5610   2.6344  ±1.47%      290
   · Products in order 10643                           604.20  1.1837   3.7374  1.6551  1.7837  2.3727  2.6485   3.7374  ±1.85%      304
   · Supplier to category through products             522.75  1.3458   7.2963  1.9130  2.0809  2.6656  2.8188   7.2963  ±2.73%      262
   · Customer orders to products                       458.96  1.5012   7.1616  2.1788  2.3641  3.6520  4.3841   7.1616  ±3.06%      230
   · Customer to category through orders and products  535.59  1.3245   6.5426  1.8671  1.9395  3.8400  3.9186   6.5426  ±2.96%      268
   · Customer to supplier through orders and products  584.92  1.2674   6.7088  1.7096  1.8099  2.6944  2.7586   6.7088  ±2.74%      293
   · Products per category                             671.99  1.1631   2.9246  1.4881  1.5654  2.4967  2.6310   2.9246  ±1.78%      337
   · Average price per category                        637.15  1.1930   2.0843  1.5695  1.6512  2.0400  2.0712   2.0843  ±1.16%      319
   · Total quantity ordered per product                576.06  1.0772  10.9084  1.7359  1.8740  2.4533  2.4658  10.9084  ±3.91%      289
   · Orders per customer                               587.25  1.0877   3.7666  1.7028  1.9158  2.6610  2.6742   3.7666  ±2.47%      295
   · Products per supplier                             584.63  1.1368   2.6405  1.7105  1.9046  2.4006  2.5678   2.6405  ±1.78%      293
   · Top products by revenue (price * quantity)        641.82  1.1231   2.4571  1.5581  1.6714  2.1791  2.2369   2.4571  ±1.64%      321
   · Products out of stock                             696.66  1.0030   2.4076  1.4354  1.5528  2.0956  2.2323   2.4076  ±1.55%      349
   · Expensive products (price > 30)                   806.91  0.9041   2.0666  1.2393  1.3536  1.8642  1.9211   2.0666  ±1.62%      404
   · Categories with product lists                     898.21  0.9183   1.9228  1.1133  1.1653  1.6228  1.6759   1.9228  ±1.11%      450
   · Customers with order lists                        677.02  1.0284   2.4269  1.4771  1.6032  2.1496  2.2202   2.4269  ±1.58%      339
   · Products with or without orders                   824.62  0.8928   1.9579  1.2127  1.3059  1.6785  1.9093   1.9579  ±1.27%      413
   · Create and delete product node                    471.47  1.3206   3.5966  2.1210  2.4630  3.3386  3.5355   3.5966  ±2.87%      236
   · Create and delete relationship                    690.50  1.0057  11.1188  1.4482  1.5816  2.2959  7.2734  11.1188  ±4.90%      346
stdout | testing\benchmarks\nornicdb-vs-neo4j-northwind.bench.ts:326:11
✓ Cleanup complete



 BENCH  Summary

  Top products by revenue (price * quantity) - testing/benchmarks/nornicdb-vs-neo4j-northwind.bench.ts > NornicDB Benchmarks (Northwind)
    1.22x faster than Expensive products (price > 30)
    1.25x faster than Products out of stock
    1.27x faster than Products in order 10643
    1.28x faster than Products in Beverages category
    1.35x faster than Find category by name
    1.36x faster than Products supplied by Exotic Liquids
    1.45x faster than Get all categories
    1.72x faster than Find product by name
    1.81x faster than Products per supplier
    1.85x faster than Find customer by ID
    1.86x faster than Customer orders to products
    1.89x faster than Supplier to category through products
    1.90x faster than Get all products
    2.00x faster than Orders by customer ALFKI
    2.01x faster than Get all customers
    2.23x faster than Orders per customer
    2.29x faster than Customers with order lists
    2.41x faster than Customer to supplier through orders and products
    2.47x faster than Customer to category through orders and products
    2.81x faster than Products per category
    2.84x faster than Categories with product lists
    3.13x faster than Average price per category
    6.28x faster than Products with or without orders
    6.40x faster than Total quantity ordered per product
    8.93x faster than Count all nodes
    19.48x faster than Count all relationships
    19.66x faster than Create and delete product node
    NaNx faster than Create and delete relationship

  Categories with product lists - testing/benchmarks/nornicdb-vs-neo4j-northwind.bench.ts > Neo4j Benchmarks (Northwind)                                                                      
    1.09x faster than Products with or without orders
    1.11x faster than Expensive products (price > 30)
    1.29x faster than Products out of stock
    1.30x faster than Create and delete relationship
    1.33x faster than Customers with order lists
    1.34x faster than Products per category
    1.40x faster than Top products by revenue (price * quantity)
    1.41x faster than Average price per category
    1.48x faster than Products supplied by Exotic Liquids
    1.49x faster than Products in order 10643
    1.53x faster than Orders per customer
    1.54x faster than Customer to supplier through orders and products
    1.54x faster than Products per supplier
    1.55x faster than Orders by customer ALFKI
    1.56x faster than Total quantity ordered per product
    1.68x faster than Customer to category through orders and products
    1.70x faster than Find customer by ID
    1.71x faster than Find product by name
    1.72x faster than Supplier to category through products
    1.76x faster than Products in Beverages category
    1.89x faster than Find category by name
    1.91x faster than Create and delete product node
    2.29x faster than Customers with order lists
    2.41x faster than Customer to supplier through orders and products
    2.47x faster than Customer to category through orders and products
    2.81x faster than Products per category
    2.84x faster than Categories with product lists
    3.13x faster than Average price per category
    6.28x faster than Products with or without orders
    6.40x faster than Total quantity ordered per product
    8.93x faster than Count all nodes
    19.48x faster than Count all relationships
    19.66x faster than Create and delete product node
    NaNx faster than Create and delete relationship

  Categories with product lists - testing/benchmarks/nornicdb-vs-neo4j-northwind.bench.ts > Neo4j Benchmarks (Northwind)                                                                      
    1.09x faster than Products with or without orders
    1.11x faster than Expensive products (price > 30)
    1.29x faster than Products out of stock
    1.30x faster than Create and delete relationship
    1.33x faster than Customers with order lists
    1.34x faster than Products per category
    1.40x faster than Top products by revenue (price * quantity)
    1.41x faster than Average price per category
    1.48x faster than Products supplied by Exotic Liquids
    1.49x faster than Products in order 10643
    1.53x faster than Orders per customer
    1.54x faster than Customer to supplier through orders and products
    1.54x faster than Products per supplier
    1.55x faster than Orders by customer ALFKI
    1.56x faster than Total quantity ordered per product
    1.68x faster than Customer to category through orders and products
    1.70x faster than Find customer by ID
    1.71x faster than Find product by name
    1.72x faster than Supplier to category through products
    1.76x faster than Products in Beverages category
    1.89x faster than Find category by name
    1.91x faster than Create and delete product node
    6.40x faster than Total quantity ordered per product
    8.93x faster than Count all nodes
    19.48x faster than Count all relationships
    19.66x faster than Create and delete product node
    NaNx faster than Create and delete relationship

  Categories with product lists - testing/benchmarks/nornicdb-vs-neo4j-northwind.bench.ts > Neo4j Benchmarks (Northwind)                                                                      
    1.09x faster than Products with or without orders
    1.11x faster than Expensive products (price > 30)
    1.29x faster than Products out of stock
    1.30x faster than Create and delete relationship
    1.33x faster than Customers with order lists
    1.34x faster than Products per category
    1.40x faster than Top products by revenue (price * quantity)
    1.41x faster than Average price per category
    1.48x faster than Products supplied by Exotic Liquids
    1.49x faster than Products in order 10643
    1.53x faster than Orders per customer
    1.54x faster than Customer to supplier through orders and products
    1.54x faster than Products per supplier
    1.55x faster than Orders by customer ALFKI
    1.56x faster than Total quantity ordered per product
    1.68x faster than Customer to category through orders and products
    1.70x faster than Find customer by ID
    1.71x faster than Find product by name
    1.72x faster than Supplier to category through products
    1.76x faster than Products in Beverages category
    1.89x faster than Find category by name
    1.91x faster than Create and delete product node
  Categories with product lists - testing/benchmarks/nornicdb-vs-neo4j-northwind.bench.ts > Neo4j Benchmarks (Northwind)                                                                      
    1.09x faster than Products with or without orders
    1.11x faster than Expensive products (price > 30)
    1.29x faster than Products out of stock
    1.30x faster than Create and delete relationship
    1.33x faster than Customers with order lists
    1.34x faster than Products per category
    1.40x faster than Top products by revenue (price * quantity)
    1.41x faster than Average price per category
    1.48x faster than Products supplied by Exotic Liquids
    1.49x faster than Products in order 10643
    1.53x faster than Orders per customer
    1.54x faster than Customer to supplier through orders and products
    1.54x faster than Products per supplier
    1.55x faster than Orders by customer ALFKI
    1.56x faster than Total quantity ordered per product
    1.68x faster than Customer to category through orders and products
    1.70x faster than Find customer by ID
    1.71x faster than Find product by name
    1.72x faster than Supplier to category through products
    1.76x faster than Products in Beverages category
    1.89x faster than Find category by name
    1.91x faster than Create and delete product node
    1.40x faster than Top products by revenue (price * quantity)
    1.41x faster than Average price per category
    1.48x faster than Products supplied by Exotic Liquids
    1.49x faster than Products in order 10643
    1.53x faster than Orders per customer
    1.54x faster than Customer to supplier through orders and products
    1.54x faster than Products per supplier
    1.55x faster than Orders by customer ALFKI
    1.56x faster than Total quantity ordered per product
    1.68x faster than Customer to category through orders and products
    1.70x faster than Find customer by ID
    1.71x faster than Find product by name
    1.72x faster than Supplier to category through products
    1.76x faster than Products in Beverages category
    1.89x faster than Find category by name
    1.91x faster than Create and delete product node
    1.54x faster than Customer to supplier through orders and products
    1.54x faster than Products per supplier
    1.55x faster than Orders by customer ALFKI
    1.56x faster than Total quantity ordered per product
    1.68x faster than Customer to category through orders and products
    1.70x faster than Find customer by ID
    1.71x faster than Find product by name
    1.72x faster than Supplier to category through products
    1.76x faster than Products in Beverages category
    1.89x faster than Find category by name
    1.91x faster than Create and delete product node
    1.68x faster than Customer to category through orders and products
    1.70x faster than Find customer by ID
    1.71x faster than Find product by name
    1.72x faster than Supplier to category through products
    1.76x faster than Products in Beverages category
    1.89x faster than Find category by name
    1.91x faster than Create and delete product node
    1.72x faster than Supplier to category through products
    1.76x faster than Products in Beverages category
    1.89x faster than Find category by name
    1.91x faster than Create and delete product node
    1.91x faster than Create and delete product node
    1.95x faster than Get all customers
    1.96x faster than Customer orders to products
    2.49x faster than Get all products
    2.70x faster than Count all relationships
    2.76x faster than Get all categories
    3.57x faster than Count all nodes


    PS C:\Users\timot\Documents\GitHub\Mimir> npm run bench:fastrp
npm warn Unknown project config "always-auth". This will stop working in the next major version of npm.

> mimir@1.0.0 bench:fastrp
> npx vitest bench testing/benchmarks/nornicdb-vs-neo4j-fastrp.bench.ts --run

npm warn Unknown project config "always-auth". This will stop working in the next major version of npm.
Benchmarking is an experimental feature.
Breaking changes might not follow SemVer, please pin Vitest's version when using it.

 RUN  v3.2.4 C:/Users/timot/Documents/GitHub/Mimir

stdout | testing\benchmarks\nornicdb-vs-neo4j-fastrp.bench.ts:153:11

╔════════════════════════════════════════════════════════════════════╗

stdout | testing\benchmarks\nornicdb-vs-neo4j-fastrp.bench.ts:154:11
║         FastRP Node Embeddings Benchmark Suite                     ║

stdout | testing\benchmarks\nornicdb-vs-neo4j-fastrp.bench.ts:155:11
║         NornicDB vs Neo4j (with Graph Data Science)                ║

stdout | testing\benchmarks\nornicdb-vs-neo4j-fastrp.bench.ts:156:11
╚════════════════════════════════════════════════════════════════════╝


stdout | testing\benchmarks\nornicdb-vs-neo4j-fastrp.bench.ts:159:11
Connecting to NornicDB at bolt://localhost:7687...

stdout | testing\benchmarks\nornicdb-vs-neo4j-fastrp.bench.ts:164:13
✓ Connected to NornicDB

stdout | testing\benchmarks\nornicdb-vs-neo4j-fastrp.bench.ts:166:13
Loading social network dataset into NornicDB...

stdout | testing\benchmarks\nornicdb-vs-neo4j-fastrp.bench.ts:169:13
  → 20 people created in NornicDB

stdout | testing\benchmarks\nornicdb-vs-neo4j-fastrp.bench.ts:172:13
Checking Graph Data Science (GDS) support in NornicDB...

stdout | testing\benchmarks\nornicdb-vs-neo4j-fastrp.bench.ts:182:15
  ⚠️  NornicDB does not support GDS procedures (expected for drop-in replacement)

stdout | testing\benchmarks\nornicdb-vs-neo4j-fastrp.bench.ts:190:11

Connecting to Neo4j at bolt://localhost:7688...

stdout | testing\benchmarks\nornicdb-vs-neo4j-fastrp.bench.ts:195:13
✓ Connected to Neo4j

stdout | testing\benchmarks\nornicdb-vs-neo4j-fastrp.bench.ts:197:13
Loading social network dataset into Neo4j...

stdout | testing\benchmarks\nornicdb-vs-neo4j-fastrp.bench.ts:200:13
  → 20 people created in Neo4j

stdout | testing\benchmarks\nornicdb-vs-neo4j-fastrp.bench.ts:203:13
Checking Graph Data Science (GDS) support in Neo4j...

stdout | testing\benchmarks\nornicdb-vs-neo4j-fastrp.bench.ts:214:15
  ⚠️  Neo4j GDS library not installed (install from https://neo4j.com/download-center/)

stdout | testing\benchmarks\nornicdb-vs-neo4j-fastrp.bench.ts:221:11

────────────────────────────────────────────────────────────────────────


stdout | testing\benchmarks\nornicdb-vs-neo4j-fastrp.bench.ts:224:13
⚠️  WARNING: Neo4j GDS library not detected. FastRP benchmarks will be skipped.

stdout | testing\benchmarks\nornicdb-vs-neo4j-fastrp.bench.ts:225:13
   Install GDS from: https://neo4j.com/download-center/



 ✓ testing/benchmarks/nornicdb-vs-neo4j-fastrp.bench.ts > NornicDB - FastRP Embeddings 1823ms
     name                                       hz     min      max    mean     p75     p99     p995     p999     rme  samples
   · Manual: Aggregate neighbor ages        409.96  1.9347  13.0070  2.4393  2.4578  4.0187   4.3034  13.0070  ±4.54%      205
   · Manual: 2-hop neighborhood features    497.33  1.4967  15.6330  2.0107  2.0175  7.0071   7.6719  15.6330  ±6.60%      249
   · Manual: Weighted neighbor aggregation  249.65  3.4832  14.6621  4.0056  4.0323  6.4742  14.6621  14.6621  ±4.60%      125
stdout | testing\benchmarks\nornicdb-vs-neo4j-fastrp.bench.ts:230:11

────────────────────────────────────────────────────────────────────────

stdout | testing\benchmarks\nornicdb-vs-neo4j-fastrp.bench.ts:231:11
Cleaning up...


 ✓ testing/benchmarks/nornicdb-vs-neo4j-fastrp.bench.ts > Neo4j - FastRP Embeddings 2450ms
     name                                        hz     min      max     mean      p75      p99     p995     p999      rme  samples
   · Manual: Aggregate neighbor ages        93.5103  5.5179  27.4169  10.6940  12.3218  27.4169  27.4169  27.4169  ±12.49%       48
   · Manual: 2-hop neighborhood features     115.95  4.1924  28.0827   8.6245   9.2320  28.0827  28.0827  28.0827  ±11.44%       58
   · Manual: Weighted neighbor aggregation   212.76  2.8029  15.1577   4.7001   5.3933   7.8214  15.1577  15.1577   ±6.21%      107
stdout | testing\benchmarks\nornicdb-vs-neo4j-fastrp.bench.ts:259:11
✓ Cleanup complete



 BENCH  Summary

  Manual: 2-hop neighborhood features - testing/benchmarks/nornicdb-vs-neo4j-fastrp.bench.ts > NornicDB - FastRP Embeddings
    1.21x faster than Manual: Aggregate neighbor ages
    1.99x faster than Manual: Weighted neighbor aggregation

  Manual: Weighted neighbor aggregation - testing/benchmarks/nornicdb-vs-neo4j-fastrp.bench.ts > Neo4j - FastRP Embeddings
    1.83x faster than Manual: 2-hop neighborhood features
    2.28x faster than Manual: Aggregate neighbor ages

PS C:\Users\timot\Documents\GitHub\Mimir> 