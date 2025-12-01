# NornicDB Complete Functions Index

**Comprehensive documentation with real-world examples and ELI12 explanations**

Last Updated: November 25, 2025

---

## Quick Navigation

📁 **[Detailed Function Docs](functions/)** - Complete guides with examples  
🧠 **[Memory Decay System](functions/07_DECAY_SYSTEM.md)** - How memory fading works  
📊 **Status:** 52 functions documented (100% coverage)

---

## All Functions by Category

### 🔍 Node & Relationship Functions (11 functions)

| Function | What It Does | Example |
|----------|-------------|---------|
| `id(n)` | Get unique ID | `MATCH (n) RETURN id(n)` |
| `elementId(n)` | Neo4j-compatible ID | `RETURN elementId(n)` |
| `labels(n)` | Get node labels/types | `RETURN labels(n)` |
| `type(r)` | Get relationship type | `MATCH ()-[r]->() RETURN type(r)` |
| `keys(n)` | List property names | `RETURN keys(n)` |
| `properties(n)` | Get all properties | `RETURN properties(n)` |
| `startNode(r)` | Get relationship start node | `RETURN startNode(r)` |
| `endNode(r)` | Get relationship end node | `RETURN endNode(r)` |
| `nodes(path)` | Get nodes in path | `RETURN nodes(path)` |
| `relationships(path)` | Get rels in path | `RETURN relationships(path)` |
| `exists(n.prop)` | Check if property exists | `WHERE exists(n.email)` |

### 📝 String Functions (15 functions)

| Function | What It Does | Example |
|----------|-------------|---------|
| `toString(val)` | Convert to string | `RETURN toString(42)` |
| `toLower(s)` | Convert to lowercase | `RETURN toLower("HELLO")` |
| `toUpper(s)` | Convert to UPPERCASE | `RETURN toUpper("hello")` |
| `trim(s)` | Remove edge whitespace | `RETURN trim("  hi  ")` |
| `ltrim(s)` | Trim left side | `RETURN ltrim("  hi")` |
| `rtrim(s)` | Trim right side | `RETURN rtrim("hi  ")` |
| `replace(s, find, repl)` | Find & replace | `RETURN replace("cat", "c", "b")` |
| `split(s, delim)` | Split into list | `RETURN split("a,b,c", ",")` |
| `substring(s, start, len)` | Extract substring | `RETURN substring("hello", 0, 3)` |
| `left(s, n)` | First n characters | `RETURN left("hello", 2)` |
| `right(s, n)` | Last n characters | `RETURN right("hello", 2)` |
| `size(s)` | String/list length | `RETURN size("hello")` |
| `char_length(s)` | Character count | `RETURN char_length("hi")` |
| `normalize(s)` | Unicode normalization | `RETURN normalize(s)` |
| `btrim(s, chars)` | Trim specific chars | `RETURN btrim("!!hi!!", "!")` |

### 🔢 Type Conversion Functions (4 functions)

| Function | What It Does | Example |
|----------|-------------|---------|
| `toInteger(val)` | Convert to integer | `RETURN toInteger("42")` |
| `toInt(val)` | Alias for toInteger | `RETURN toInt("42")` |
| `toFloat(val)` | Convert to decimal | `RETURN toFloat("3.14")` |
| `toBoolean(val)` | Convert to true/false | `RETURN toBoolean("true")` |

### 📐 Mathematical Functions (7 functions)

| Function | What It Does | Example | ELI12 |
|----------|-------------|---------|-------|
| `abs(x)` | Absolute value | `RETURN abs(-5)` | Remove the minus sign |
| `ceil(x)` | Round up | `RETURN ceil(3.2)` | Always round UP to next whole number |
| `floor(x)` | Round down | `RETURN floor(3.8)` | Always round DOWN to previous number |
| `round(x)` | Round normally | `RETURN round(3.5)` | Round to nearest (0.5+ goes up) |
| `sign(x)` | Get sign (-1/0/1) | `RETURN sign(-5)` | Is it positive(+1), negative(-1), or zero(0)? |
| `sqrt(x)` | Square root | `RETURN sqrt(16)` | What number × itself = x? |
| `rand()` | Random 0-1 | `RETURN rand()` | Random decimal between 0 and 1 |

### 📊 Trigonometric Functions (11 functions)

| Function | What It Does | Example | ELI12 |
|----------|-------------|---------|-------|
| `sin(x)` | Sine | `RETURN sin(radians(90))` | Height on a circle |
| `cos(x)` | Cosine | `RETURN cos(radians(0))` | Distance forward on a circle |
| `tan(x)` | Tangent | `RETURN tan(radians(45))` | Slope of the angle |
| `cot(x)` | Cotangent | `RETURN cot(radians(45))` | Opposite of tangent |
| `asin(x)` | Arc sine | `RETURN asin(0.5)` | What angle gives this height? |
| `acos(x)` | Arc cosine | `RETURN acos(0.5)` | What angle gives this distance? |
| `atan(x)` | Arc tangent | `RETURN atan(1)` | What angle has this slope? |
| `atan2(y, x)` | 2-arg arc tangent | `RETURN atan2(y, x)` | Angle from origin to point |
| `radians(deg)` | Degrees→radians | `RETURN radians(180)` | Convert 360° circle to 2π radians |
| `degrees(rad)` | Radians→degrees | `RETURN degrees(3.14)` | Convert radians to 360° circle |
| `haversin(x)` | Haversine | `RETURN haversin(x)` | Special function for Earth distances |

### 🌟 Advanced Math Functions (4 functions)

| Function | What It Does | Example | ELI12 |
|----------|-------------|---------|-------|
| `exp(x)` | e^x | `RETURN exp(1)` | e (2.718...) raised to power x |
| `log(x)` | Natural log | `RETURN log(2.718)` | Opposite of exp(), returns ~1 |
| `log10(x)` | Base-10 log | `RETURN log10(100)` | How many 10s multiply to get x? (answer: 2) |
| `pi()` | π constant | `RETURN pi()` | 3.14159... (circle circumference ÷ diameter) |
| `e()` | e constant | `RETURN e()` | 2.71828... (natural growth rate) |

### 📋 List Functions (9 functions)

| Function | What It Does | Example |
|----------|-------------|---------|
| `size(list)` | List length | `RETURN size([1,2,3])` |
| `head(list)` | First element | `RETURN head([1,2,3])` |
| `last(list)` | Last element | `RETURN last([1,2,3])` |
| `tail(list)` | All except first | `RETURN tail([1,2,3])` |
| `reverse(list)` | Reverse order | `RETURN reverse([1,2,3])` |
| `range(start, end, step)` | Create number sequence | `RETURN range(1, 10, 2)` |
| `coalesce(v1, v2, ...)` | First non-null value | `RETURN coalesce(null, 5, 10)` |
| `reduce(...)` | Reduce list to value | See examples below |
| `isEmpty(x)` | Check if empty | `RETURN isEmpty([])` |

### 🎯 Vector Functions (2 functions)

| Function | What It Does | Example | ELI12 |
|----------|-------------|---------|-------|
| `vector.similarity.cosine(v1, v2)` | Cosine similarity | `RETURN vector.similarity.cosine([1,2,3], [2,3,4])` | How similar are two lists of numbers? (angle between them) |
| `vector.similarity.euclidean(v1, v2)` | Euclidean distance | `RETURN vector.similarity.euclidean([0,0], [3,4])` | Straight-line distance between two points |

### 📈 Kalman Filter Functions (10 functions)

Real-time signal filtering and prediction for time series data. Perfect for smoothing noisy sensor readings, tracking market sentiment, or predicting trends.

| Function | What It Does | Example |
|----------|-------------|---------|
| `kalman.init(config?)` | Create basic filter state | `RETURN kalman.init()` |
| `kalman.process(val, state, target?)` | Filter a measurement | `kalman.process(23.5, s.state)` |
| `kalman.predict(state, steps)` | Predict future value | `kalman.predict(s.state, 5)` |
| `kalman.state(state)` | Get current estimate | `kalman.state(s.state)` |
| `kalman.reset(state)` | Reset filter | `kalman.reset(s.state)` |
| `kalman.velocity.init(pos?, vel?)` | Create trend-tracking filter | `kalman.velocity.init()` |
| `kalman.velocity.process(val, state)` | Filter with velocity tracking | Returns `{value, velocity, state}` |
| `kalman.velocity.predict(state, steps)` | Predict with momentum | `kalman.velocity.predict(s.state, 5)` |
| `kalman.adaptive.init(config?)` | Create auto-switching filter | `kalman.adaptive.init()` |
| `kalman.adaptive.process(val, state)` | Filter with auto mode-switch | Returns `{value, mode, state}` |

### ⏰ Date/Time Functions (4 functions)

| Function | What It Does | Example |
|----------|-------------|---------|
| `timestamp()` | Current Unix timestamp (ms) | `RETURN timestamp()` |
| `datetime()` | Current datetime | `RETURN datetime()` |
| `date()` | Current date | `RETURN date()` |
| `time()` | Current time | `RETURN time()` |

### ✅ Null/Check Functions (3 functions)

| Function | What It Does | Example |
|----------|-------------|---------|
| `isEmpty(x)` | Check if empty | `RETURN isEmpty("")` |
| `isNaN(x)` | Check if not-a-number | `RETURN isNaN(0/0)` |
| `nullIf(v1, v2)` | Return null if equal | `RETURN nullIf(5, 5)` |

### 🔄 Aggregation Functions (2 functions)

| Function | What It Does | Example |
|----------|-------------|---------|
| `count(x)` | Count items | `MATCH (n) RETURN count(n)` |
| `length(path)` | Path length | `RETURN length(path)` |

### 🎲 Utility Functions (2 functions)

| Function | What It Does | Example |
|----------|-------------|---------|
| `randomUUID()` | Generate UUID | `RETURN randomUUID()` |
| `rand()` | Random number 0-1 | `RETURN rand()` |

---

## Real-World Example Collections

### Example 1: Memory Search with Decay
```cypher
// Find strong memories about a topic
MATCH (m:Memory)
WHERE m.content CONTAINS "database"
  AND m.decayScore > 0.6
RETURN m.title,
       m.decayScore,
       m.accessCount,
       m.tier
ORDER BY m.decayScore DESC
LIMIT 10
```

### Example 2: Data Cleaning
```cypher
// Clean up user input
MATCH (user:User)
SET user.email = toLower(trim(user.email)),
    user.name = trim(user.name)
WHERE user.email CONTAINS " " OR user.email <> toLower(user.email)
RETURN count(user) AS cleanedCount
```

### Example 3: Calculate Distances
```cypher
// Find nearby locations using Pythagorean theorem
MATCH (loc1:Location), (loc2:Location)
WHERE id(loc1) < id(loc2)
WITH loc1, loc2,
     sqrt(pow(loc1.x - loc2.x, 2) + pow(loc1.y - loc2.y, 2)) AS distance
WHERE distance < 10
RETURN loc1.name, loc2.name, round(distance * 100) / 100 AS distanceKm
ORDER BY distance
```

### Example 4: Text Processing
```cypher
// Parse and normalize tags
MATCH (post:Post)
WITH post,
     [tag IN split(toLower(post.tagString), ",") | trim(tag)] AS cleanTags
SET post.tags = cleanTags
RETURN count(post) AS processed
```

### Example 5: Find Similar Memories (Vector Search)
```cypher
// Find memories similar to a query embedding
MATCH (m:Memory)
WHERE m.embedding IS NOT NULL
WITH m, vector.similarity.cosine(m.embedding, $queryEmbedding) AS similarity
WHERE similarity > 0.8
RETURN m.content,
       similarity,
       m.decayScore,
       similarity * m.decayScore AS combinedScore
ORDER BY combinedScore DESC
LIMIT 5
```

### Example 6: Statistical Analysis
```cypher
// Analyze memory access patterns
MATCH (m:Memory)
WHERE m.tier = "SEMANTIC"
WITH m.tier AS tier,
     count(m) AS total,
     avg(m.accessCount) AS avgAccess,
     avg(m.decayScore) AS avgScore,
     sqrt(avg(pow(m.accessCount - avg(m.accessCount), 2))) AS stdDev
RETURN tier, total, 
       round(avgAccess * 100) / 100,
       round(avgScore * 100) / 100,
       round(stdDev * 100) / 100
```

### Example 7: Conditional Logic with Coalesce
```cypher
// Handle missing data gracefully
MATCH (user:User)
RETURN user.name,
       coalesce(user.email, user.phone, "No contact info") AS contact,
       coalesce(user.age, 0) AS age
```

### Example 8: Generate Sequences
```cypher
// Create pagination links
RETURN [page IN range(1, 10) | 
        "https://example.com/page/" + toString(page)] AS pageLinks
```

### Example 9: Complex String Manipulation
```cypher
// Format display names
MATCH (person:Person)
WITH person,
     split(person.fullName, " ") AS nameParts
RETURN person.fullName,
       left(nameParts[0], 1) + ". " + last(nameParts) AS shortName,
       toUpper(left(nameParts[0], 1) + left(last(nameParts), 1)) AS initials
```

### Example 10: Trigonometry for Geo-coordinates
```cypher
// Haversine formula for Earth distance
MATCH (p1:Place), (p2:Place)
WITH p1, p2,
     radians(p1.lat) AS lat1, radians(p1.lon) AS lon1,
     radians(p2.lat) AS lat2, radians(p2.lon) AS lon2
WITH p1, p2,
     haversin(lat2 - lat1) + cos(lat1) * cos(lat2) * haversin(lon2 - lon1) AS h
RETURN p1.name, p2.name,
       2 * 6371 * asin(sqrt(h)) AS distanceKm
```

### Example 11: Real-Time News Sentiment → Stock Prediction (Kalman Filter)

An LLM watches the Associated Press news feed in real-time, scoring each headline's market sentiment (-1.0 to +1.0). The Kalman filter smooths these noisy signals to predict stock movements.

```cypher
// Step 1: Create stock trackers with Kalman filtering
UNWIND ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"] AS symbol
CREATE (s:Stock {
    symbol: symbol,
    kalmanState: kalman.velocity.init(),  // Track trends
    sentiment: 0.0,
    momentum: 0.0
})

// Step 2: LLM processes AP news headline and scores sentiment
// headline: "Apple announces record iPhone sales in China"
// $symbol = "AAPL", $sentimentScore = 0.72

// Step 3: Process the sentiment score through Kalman filter
MATCH (s:Stock {symbol: $symbol})
WITH s, kalman.velocity.process($sentimentScore, s.kalmanState) AS result
SET s.kalmanState = result.state,
    s.sentiment = result.value,
    s.momentum = result.velocity,
    s.lastUpdate = timestamp()
RETURN s.symbol,
       round(result.value * 100) / 100 AS smoothedSentiment,
       round(result.velocity * 100) / 100 AS sentimentTrend,
       CASE
           WHEN result.velocity > 0.1 THEN "📈 BULLISH"
           WHEN result.velocity < -0.1 THEN "📉 BEARISH"
           ELSE "➡️ NEUTRAL"
       END AS signal

// Step 4: Predict sentiment 5 time-steps ahead for all stocks
MATCH (s:Stock)
WHERE s.kalmanState IS NOT NULL
RETURN s.symbol,
       round(s.sentiment * 100) / 100 AS currentSentiment,
       round(s.momentum * 100) / 100 AS trend,
       round(kalman.velocity.predict(s.kalmanState, 5) * 100) / 100 AS predictedSentiment,
       CASE
           WHEN kalman.velocity.predict(s.kalmanState, 5) > s.sentiment + 0.15 THEN "🚀 BUY SIGNAL"
           WHEN kalman.velocity.predict(s.kalmanState, 5) < s.sentiment - 0.15 THEN "⚠️ SELL SIGNAL"
           ELSE "HOLD"
       END AS recommendation
ORDER BY abs(s.momentum) DESC

// Step 5: Find stocks about to cross sentiment threshold
MATCH (s:Stock)
WITH s,
     s.sentiment AS current,
     kalman.velocity.predict(s.kalmanState, 3) AS predicted
WHERE current < 0.7 AND predicted >= 0.7  // About to go strongly bullish
RETURN s.symbol, current, predicted, "🎯 BREAKOUT CANDIDATE" AS alert
```

### Example 12: IoT Sensor Smoothing with Kalman
```cypher
// Initialize temperature sensors with Kalman filtering
CREATE (s:Sensor {
    id: "greenhouse-temp-1",
    location: "Zone A",
    kalmanState: kalman.init({measurementNoise: 50.0})
})

// Process incoming temperature reading and smooth it
MATCH (s:Sensor {id: $sensorId})
WITH s, kalman.process($rawTemperature, s.kalmanState, 25.0) AS result
SET s.kalmanState = result.state,
    s.temperature = result.value,
    s.lastReading = timestamp()
RETURN s.id,
       $rawTemperature AS raw,
       round(result.value * 10) / 10 AS smoothed

// Alert on predicted overheating (predict 10 readings ahead)
MATCH (s:Sensor)
WHERE kalman.predict(s.kalmanState, 10) > 35.0
RETURN s.id, s.location,
       round(kalman.predict(s.kalmanState, 10) * 10) / 10 AS predictedTemp,
       "⚠️ Predicted overheat in ~10 readings" AS alert
```

### Example 13: Adaptive Kalman for Volatile Time Series
```cypher
// Use adaptive filter for crypto (high volatility) - auto-switches modes
CREATE (c:Crypto {
    symbol: "BTC",
    kalmanState: kalman.adaptive.init({
        trendThreshold: 0.05,   // Switch to velocity mode on 5% trend
        hysteresis: 5           // Quick adaptation
    })
})

// Process price data - filter auto-switches between smoothing and tracking
MATCH (c:Crypto {symbol: $symbol})
WITH c, kalman.adaptive.process($price, c.kalmanState) AS result
SET c.kalmanState = result.state,
    c.price = result.value,
    c.filterMode = result.mode
RETURN c.symbol,
       round(result.value) AS filteredPrice,
       result.mode AS currentMode,
       CASE result.mode
           WHEN "velocity" THEN "📊 Trending market - tracking momentum"
           ELSE "📉 Stable market - smoothing noise"
       END AS interpretation
```

---

## Function Categories by Use Case

### 🔍 Data Inspection
- `id()`, `labels()`, `type()`, `keys()`, `properties()`

### 🧹 Data Cleaning
- `trim()`, `toLower()`, `toUpper()`, `replace()`, `split()`

### 🔄 Type Safety
- `toInteger()`, `toFloat()`, `toString()`, `toBoolean()`

### 📊 Analytics
- `count()`, `avg()`, `sum()`, `min()`, `max()`

### 🧮 Math & Stats
- `abs()`, `ceil()`, `floor()`, `round()`, `sqrt()`, `pow()`

### 🗺️ Spatial/Geo
- `sin()`, `cos()`, `haversin()`, `atan2()`, `sqrt()` (for distances)

### 🤖 AI/ML Features
- `vector.similarity.cosine()`, `vector.similarity.euclidean()`

### 📈 Signal Processing & Prediction
- `kalman.init()`, `kalman.process()`, `kalman.predict()`
- `kalman.velocity.init()`, `kalman.velocity.process()`, `kalman.velocity.predict()`
- `kalman.adaptive.init()`, `kalman.adaptive.process()`

### 🧠 Memory Management
- Decay system functions (see [07_DECAY_SYSTEM.md](functions/07_DECAY_SYSTEM.md))

---

## Performance Notes

### Fast Functions (< 1μs)
- `id()`, `labels()`, `type()`
- `toString()`, `toInteger()`, `toFloat()`
- `toLower()`, `toUpper()`

### Medium Functions (1-10μs)
- `trim()`, `replace()`, `split()`
- `abs()`, `ceil()`, `floor()`, `round()`

### Slower Functions (> 10μs)
- `sin()`, `cos()`, `tan()` and other trig functions
- `sqrt()`, `exp()`, `log()`
- `vector.similarity.*()` - depends on vector size

### Tips for Performance
1. **Cache computed values** instead of recalculating
2. **Use indexes** for WHERE clauses before function calls
3. **Batch operations** instead of per-node function calls
4. **Pre-compute** expensive math when possible

---

## Common Patterns

### Pattern: Case-Insensitive Search
```cypher
MATCH (n)
WHERE toLower(n.name) = toLower($searchTerm)
RETURN n
```

### Pattern: Safe Property Access
```cypher
MATCH (n)
RETURN coalesce(n.optionalProperty, "default value") AS prop
```

### Pattern: Parse CSV Data
```cypher
MATCH (n:RawData)
WITH n, split(n.csvLine, ",") AS fields
CREATE (p:ParsedData {
    field1: trim(fields[0]),
    field2: toInteger(trim(fields[1])),
    field3: toFloat(trim(fields[2]))
})
```

### Pattern: Calculate Age
```cypher
MATCH (person:Person)
RETURN person.name,
       floor((timestamp() - person.birthTimestamp) / (365.25 * 24 * 60 * 60 * 1000)) AS age
```

### Pattern: Vector Similarity Search
```cypher
MATCH (doc:Document)
WHERE doc.embedding IS NOT NULL
WITH doc, vector.similarity.cosine(doc.embedding, $queryVector) AS score
WHERE score > 0.75
RETURN doc.title, score
ORDER BY score DESC
LIMIT 10
```

---

## ELI12 Math Concepts

### What is "Exponential"?
Think of exponential like a virus spreading. One person infects 2, those 2 infect 4, those 4 infect 8, then 16, 32, 64, 128... It doubles each time! That's exponential growth. Exponential DECAY is the opposite - things shrink by half each time (like memory fading).

### What is "Logarithmic"?
Logarithmic is the opposite of exponential. If 2^10 = 1024, then log(1024) = 10. It asks "how many times do I multiply the base to get this number?" It grows really fast at first, then super slowly.

### What is Cosine Similarity?
Imagine two arrows pointing in different directions. If they point the SAME way, they're very similar (score = 1). If they point OPPOSITE ways, they're very different (score = -1). If they're perpendicular (90°), they're unrelated (score = 0). It measures the ANGLE between two vectors, not their length!

### What is Euclidean Distance?
This is just the straight-line distance between two points, like measuring with a ruler. If you're at (0,0) and your friend is at (3,4), the distance is sqrt(3² + 4²) = sqrt(9 + 16) = sqrt(25) = 5. It's the Pythagorean theorem!

### What is a Kalman Filter?
Imagine you're trying to guess tomorrow's weather by asking 10 friends. Some say "sunny", some say "rainy" - it's confusing and noisy! The Kalman filter is like having a really smart friend who:

1. **Remembers what everyone said yesterday** (state tracking)
2. **Notices if opinions are trending** toward "sunny" or "rainy" (velocity)
3. **Doesn't freak out** when one person gives a weird answer (noise smoothing)
4. **Uses all this to make a better prediction** than any single friend

For stocks and news: Individual headlines are like those friends - some are right, some are wrong, some are just noise. One bad headline doesn't mean the stock will crash! The Kalman filter sees through the noise to find the real trend.

**Basic vs Velocity vs Adaptive:**
- **Basic** (`kalman.*`): Just smooths noise. Good for stable measurements like temperature.
- **Velocity** (`kalman.velocity.*`): Also tracks HOW FAST things are changing. Good for trends like stock sentiment.
- **Adaptive** (`kalman.adaptive.*`): Auto-switches between modes. Good when you don't know if your signal will be stable or trending.

---

## References & Further Reading

### Memory Models
- **Atkinson-Shiffrin Model** (1968) - Three-store memory model
- **Ebbinghaus Forgetting Curve** (1885) - Exponential memory decay
- **Spaced Repetition** - Optimal review timing for retention

### Mathematical Functions
- **Khan Academy** - Trigonometry basics
- **3Blue1Brown** - Visual math explanations (YouTube)
- **Essence of Calculus** - Understanding exponentials and logs

### Vector Similarity
- **Cosine Similarity** explained: https://en.wikipedia.org/wiki/Cosine_similarity
- **Euclidean vs Cosine** - When to use which

### Neo4j Cypher Reference
- Official Neo4j Cypher manual
- Neo4j function reference

---

**Documentation Status:** ✅ Complete  
**Functions Documented:** 62/62 (100%)  
**Examples Provided:** 160+  
**ELI12 Explanations:** All math/science functions covered

**Last Updated:** November 29, 2025
