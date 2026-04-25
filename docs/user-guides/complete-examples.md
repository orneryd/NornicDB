# NornicDB Complete Working Examples

**Real-world scenarios with full code**

---

## Table of Contents

1. [AI Agent Memory System](#1-ai-agent-memory-system)
2. [Code Knowledge Base](#2-code-knowledge-base)
3. [Personal Knowledge Graph](#3-personal-knowledge-graph)
4. [Project Documentation](#4-project-documentation)
5. [Learning Tracker](#5-learning-tracker)

---

## 1. AI Agent Memory System

### Scenario
An AI coding assistant needs to remember user preferences, project decisions, and context across sessions.

### Step 1: Define Decay Profiles

```cypher
// Short-lived context — fades in about a week
CREATE DECAY PROFILE episode_retention
  HALF LIFE 604800
  DECAY FLOOR 0.0
  VISIBILITY THRESHOLD 0.10;

// Stable knowledge — fades over months
CREATE DECAY PROFILE fact_retention
  HALF LIFE 5961600
  DECAY FLOOR 0.05
  VISIBILITY THRESHOLD 0.10;

// Persistent skills — no decay
CREATE DECAY PROFILE skill_retention
  NO DECAY;
```

### Step 2: Bind Profiles to Labels

```cypher
CREATE RETENTION BINDING episode_binding
  FOR (n:MemoryEpisode)
  USING PROFILE episode_retention;

CREATE RETENTION BINDING fact_binding
  FOR (n:KnowledgeFact)
  USING PROFILE fact_retention
  PROPERTY n.tenantId NO DECAY
  PROPERTY n.summary HALF LIFE 2592000;

CREATE RETENTION BINDING skill_binding
  FOR (n:Skill)
  USING PROFILE skill_retention;
```

### Step 3: Create Nodes

```cypher
CREATE (pref:KnowledgeFact {
  id: randomUUID(),
  content: "User prefers TypeScript over JavaScript",
  tags: ["preference", "language"],
  created: timestamp()
})

CREATE (decision:KnowledgeFact {
  id: randomUUID(),
  content: "Project uses React 18 with Vite for frontend",
  tags: ["decision", "architecture"],
  created: timestamp()
})

CREATE (practice:Skill {
  id: randomUUID(),
  content: "Always use async/await instead of .then() chains",
  tags: ["best-practice", "async"],
  created: timestamp()
})

CREATE (chat:MemoryEpisode {
  id: randomUUID(),
  content: "Currently debugging authentication middleware",
  tags: ["context", "debugging"],
  created: timestamp()
})
```

### Step 4: Query with Decay-Aware Scoring

```cypher
// Get strong memories about preferences
MATCH (m:KnowledgeFact)
WHERE "preference" IN m.tags
  AND decayScore(m) > 0.5
RETURN m.content, decayScore(m)
ORDER BY decayScore(m) DESC

// Get current context (episodes still above visibility threshold)
MATCH (m:MemoryEpisode)
WHERE decayScore(m) > 0.10
RETURN m.content, m.created
ORDER BY m.created DESC
LIMIT 5

// Semantic search weighted by decay
MATCH (m:KnowledgeFact)
WHERE m.embedding IS NOT NULL
  AND decayScore(m) > 0.4
WITH m, vector.similarity.cosine(m.embedding, $queryEmbedding) AS similarity
WHERE similarity > 0.75
RETURN m.content,
       similarity,
       decayScore(m),
       similarity * decayScore(m) AS combinedScore
ORDER BY combinedScore DESC
LIMIT 5
```

### Expected Results

```
// Preferences (KnowledgeFact — slow decay)
├─ "User prefers TypeScript over JavaScript" (Score: 0.68)
└─ "User likes functional programming style" (Score: 0.62)

// Architecture decisions (KnowledgeFact)
├─ "Project uses React 18 with Vite for frontend" (Score: 0.91)

// Current context (MemoryEpisode — fast decay, still visible)
├─ "Currently debugging authentication middleware" (Score: 0.35)

// Skills (NO DECAY — always score 1.0)
├─ "Always use async/await" (Score: 1.00)
```

---

## 2. Code Knowledge Base

### Scenario
Track code patterns, bugs, and solutions across a codebase.

```cypher
// === Store code pattern ===
CREATE (pattern:KnowledgeFact {
  id: randomUUID(),
  content: "Use Zod for runtime type validation in API routes",
  codeExample: "const schema = z.object({ email: z.string().email() })",
  tags: ["pattern", "validation", "zod", "api"],
  file: "src/api/users.ts",
  created: timestamp(),
  importance: 0.8
})

// === Store bug and solution ===
CREATE (bug:MemoryEpisode {
  id: randomUUID(),
  content: "Race condition in useEffect caused double API calls",
  solution: "Added cleanup function: return () => { cancelled = true }",
  tags: ["bug", "react", "useEffect", "race-condition"],
  file: "src/hooks/useData.ts",
  created: timestamp(),
  importance: 0.6
})

// === Link related patterns ===
MATCH (p1:KnowledgeFact), (p2:KnowledgeFact)
WHERE "validation" IN p1.tags
  AND "api" IN p2.tags
  AND id(p1) < id(p2)
CREATE (p1)-[:RELATES_TO {
  reason: "Both deal with API input handling",
  confidence: 0.80
}]->(p2)

// === Search by file ===
MATCH (m:KnowledgeFact)
WHERE m.file = "src/api/users.ts"
RETURN m.content, m.codeExample, m.tags
ORDER BY m.importance DESC

// === Find similar bugs ===
MATCH (bug:MemoryEpisode)
WHERE "bug" IN bug.tags
  AND bug.content CONTAINS "useEffect"
RETURN bug.content, bug.solution, bug.file

// === Get all patterns for a technology ===
MATCH (m:KnowledgeFact)
WHERE "zod" IN m.tags
RETURN m.content, m.codeExample
```

---

## 3. Personal Knowledge Graph

### Scenario
Build a second brain for personal learning and note-taking.

```cypher
// === Create topic nodes ===
CREATE (ai:Topic {
  name: "Artificial Intelligence",
  category: "Technology"
})

CREATE (ml:Topic {
  name: "Machine Learning",
  category: "Technology"
})

CREATE (nn:Topic {
  name: "Neural Networks",
  category: "Technology"
})

// === Create learning nodes ===
CREATE (concept:KnowledgeFact {
  id: randomUUID(),
  content: "Backpropagation calculates gradients using chain rule",
  tags: ["ml", "neural-networks", "concept"],
  source: "Deep Learning book, Chapter 6",
  created: timestamp(),
  importance: 0.8
})

CREATE (practice:KnowledgeFact {
  id: randomUUID(),
  content: "Always normalize input features before training",
  tags: ["ml", "best-practice", "preprocessing"],
  created: timestamp(),
  importance: 0.9
})

// === Link topics hierarchically ===
CREATE (ai)-[:CONTAINS]->(ml)
CREATE (ml)-[:CONTAINS]->(nn)

// === Link knowledge to topics ===
MATCH (m:KnowledgeFact), (t:Topic)
WHERE "neural-networks" IN m.tags
  AND t.name = "Neural Networks"
CREATE (m)-[:ABOUT]->(t)

// === Query: What do I know about ML? ===
MATCH (topic:Topic {name: "Machine Learning"})-[:CONTAINS*0..2]->(subtopic)
MATCH (m:KnowledgeFact)-[:ABOUT]->(subtopic)
WHERE decayScore(m) > 0.5
RETURN subtopic.name AS topic,
       collect(m.content) AS concepts,
       avg(decayScore(m)) AS avgRetention
ORDER BY avgRetention DESC

// === Query: Spaced repetition - what should I review? ===
MATCH (m:KnowledgeFact)
WHERE decayScore(m) BETWEEN 0.3 AND 0.6
  AND "concept" IN m.tags
RETURN m.content,
       m.source,
       decayScore(m)
ORDER BY decayScore(m) ASC
LIMIT 5

// === Access reinforces decay score automatically ===
MATCH (m:KnowledgeFact)
WHERE m.content CONTAINS "Backpropagation"
RETURN m.content, decayScore(m)
```

---

## 4. Project Documentation

### Scenario
Auto-document project decisions and architecture using the graph.

```cypher
// === Create project structure ===
CREATE (proj:Project {
  name: "MyProject",
  description: "AI Knowledge Management System"
})

CREATE (frontend:Component {
  name: "Frontend",
  tech: "React + TypeScript",
  path: "src/ui/"
})

CREATE (backend:Component {
  name: "Backend",
  tech: "Node.js + Express",
  path: "src/api/"
})

CREATE (db:Component {
  name: "Database",
  tech: "NornicDB (Graph DB)",
  path: "nornicdb/"
})

// === Link components ===
CREATE (proj)-[:HAS_COMPONENT]->(frontend)
CREATE (proj)-[:HAS_COMPONENT]->(backend)
CREATE (proj)-[:HAS_COMPONENT]->(db)
CREATE (frontend)-[:DEPENDS_ON]->(backend)
CREATE (backend)-[:DEPENDS_ON]->(db)

// === Document decisions ===
CREATE (dec1:Decision {
  id: randomUUID(),
  title: "Use TypeScript for type safety",
  rationale: "Catches errors at compile time, better IDE support",
  date: timestamp(),
  status: "ACCEPTED",
  impact: "HIGH"
})

CREATE (dec2:Decision {
  id: randomUUID(),
  title: "Implement memory decay system",
  rationale: "Mimics human memory, auto-cleans old data",
  date: timestamp(),
  status: "IMPLEMENTED",
  impact: "MEDIUM"
})

// === Link decisions to components ===
MATCH (dec:Decision), (comp:Component)
WHERE dec.title CONTAINS "TypeScript"
  AND comp.name = "Frontend"
CREATE (comp)-[:DECIDED_BY]->(dec)

MATCH (dec:Decision), (comp:Component)
WHERE dec.title CONTAINS "memory decay"
  AND comp.name = "Database"
CREATE (comp)-[:DECIDED_BY]->(dec)

// === Generate architecture document ===
MATCH (proj:Project)-[:HAS_COMPONENT]->(comp:Component)
OPTIONAL MATCH (comp)-[:DECIDED_BY]->(dec:Decision)
OPTIONAL MATCH (comp)-[:DEPENDS_ON]->(dep:Component)
RETURN proj.name AS project,
       comp.name AS component,
       comp.tech AS technology,
       collect(DISTINCT dec.title) AS decisions,
       collect(DISTINCT dep.name) AS dependencies
ORDER BY comp.name

// === Find all high-impact decisions ===
MATCH (dec:Decision)
WHERE dec.impact = "HIGH"
RETURN dec.title,
       dec.rationale,
       dec.status,
       dec.date
ORDER BY dec.date DESC

// === Trace dependency chain ===
MATCH path = (start:Component)-[:DEPENDS_ON*]->(end:Component)
WHERE start.name = "Frontend"
RETURN [comp IN nodes(path) | comp.name] AS dependencyChain
```

---

## 5. Learning Tracker

### Scenario
Track what you're learning with spaced repetition.

```cypher
// === Setup: decay profile for study facts ===
CREATE DECAY PROFILE study_fact_retention
  HALF LIFE 5961600
  DECAY FLOOR 0.05
  VISIBILITY THRESHOLD 0.10;

CREATE RETENTION BINDING study_fact_binding
  FOR (n:StudyFact)
  USING PROFILE study_fact_retention;

// === Create study session ===
CREATE (session:StudySession {
  id: randomUUID(),
  topic: "Graph Algorithms",
  date: timestamp()
})

// === Add what you learned ===
CREATE (fact1:StudyFact {
  id: randomUUID(),
  content: "Dijkstra's algorithm finds shortest path in weighted graph",
  tags: ["algorithms", "graphs", "dijkstra"],
  difficulty: "MEDIUM",
  created: timestamp(),
  importance: 0.7,
  nextReview: timestamp() + (3 * 24 * 60 * 60 * 1000)
})

CREATE (fact2:StudyFact {
  id: randomUUID(),
  content: "BFS uses queue, DFS uses stack (or recursion)",
  tags: ["algorithms", "graphs", "bfs", "dfs"],
  difficulty: "EASY",
  created: timestamp(),
  importance: 0.8,
  nextReview: timestamp() + (7 * 24 * 60 * 60 * 1000)
})

// === Link to session ===
MATCH (m:StudyFact), (s:StudySession)
WHERE m.id IN ["fact1-id", "fact2-id"]
  AND s.topic = "Graph Algorithms"
CREATE (s)-[:LEARNED]->(m)

// === What to review today? (weakest decay scores first) ===
MATCH (m:StudyFact)
WHERE m.nextReview <= timestamp()
RETURN m.content,
       m.difficulty,
       decayScore(m)
ORDER BY decayScore(m) ASC
LIMIT 10

// === Mark as reviewed (spaced repetition) ===
MATCH (m:StudyFact)
WHERE m.content CONTAINS "Dijkstra"
WITH m,
     CASE m.difficulty
       WHEN "EASY" THEN 7
       WHEN "MEDIUM" THEN 3
       WHEN "HARD" THEN 1
     END AS daysUntilReview
SET m.nextReview = timestamp() + (daysUntilReview * 24 * 60 * 60 * 1000)
RETURN m.content, m.difficulty, decayScore(m)

// === Study statistics ===
MATCH (s:StudySession)-[:LEARNED]->(m:StudyFact)
WHERE s.date > timestamp() - (30 * 24 * 60 * 60 * 1000)
RETURN s.topic,
       s.date,
       count(m) AS factsLearned,
       avg(decayScore(m)) AS avgRetention,
       sum(CASE WHEN decayScore(m) > 0.7 THEN 1 ELSE 0 END) AS strongFacts
ORDER BY s.date DESC

// === Knowledge map ===
MATCH (m:StudyFact)
WITH m.tags AS tags, count(m) AS count, avg(decayScore(m)) AS avgScore
UNWIND tags AS tag
RETURN tag,
       sum(count) AS totalConcepts,
       round(avg(avgScore) * 100) / 100 AS retention
ORDER BY totalConcepts DESC
LIMIT 20
```

---

## Common Patterns Library

### Pattern: Query Decay Scores
```cypher
// Scores are computed automatically by decay profiles — query with decayScore()
MATCH (m:Memory)
WHERE decayScore(m) > 0.3
RETURN m.content, decayScore(m) AS score
ORDER BY score DESC
```

### Pattern: Find Clusters
```cypher
// Find tightly connected memories
MATCH (m:Memory)-[r:RELATES_TO]-(related:Memory)
WHERE r.confidence > 0.8
WITH m, count(related) AS connections, collect(related.content) AS cluster
WHERE connections >= 3
RETURN m.content AS centerMemory,
       connections,
       cluster
ORDER BY connections DESC
LIMIT 10
```

### Pattern: Time-based Analysis
```cypher
// Memories created per day (last 30 days)
MATCH (m:Memory)
WHERE m.created > timestamp() - (30 * 24 * 60 * 60 * 1000)
WITH toInteger(m.created / (24 * 60 * 60 * 1000)) AS day,
     count(m) AS memoriesCreated
RETURN day, memoriesCreated
ORDER BY day DESC
```

### Pattern: Confidence-weighted Search
```cypher
// Find memories with high confidence links
MATCH (m:Memory)-[r:RELATES_TO]->(related:Memory)
WHERE m.content CONTAINS $query
WITH m, related, r.confidence AS conf
ORDER BY conf DESC
RETURN m.content AS source,
       collect({content: related.content, confidence: conf})[0..5] AS relatedMemories
```

---

**Last Updated:** November 25, 2025
