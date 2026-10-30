# Creating Perfect Training Data for Your LLM

**Comprehensive guide to creating high-quality training datasets for specialized tasks.**

---

## Table of Contents

1. [General Principles](#general-principles)
2. [Dataset Size & Quality](#dataset-size--quality)
3. [Task-Specific Guides](#task-specific-guides)
   - [Cypher Query Syntax](#cypher-query-syntax-training)
   - [TLP Classification](#tlp-classification)
   - [Code Generation](#code-generation)
   - [Text Classification](#text-classification)
4. [Data Collection Methods](#data-collection-methods)
5. [Quality Assurance](#quality-assurance)
6. [Common Pitfalls](#common-pitfalls)

---

## General Principles

### The 5 Pillars of Training Data

1. **Accuracy** - Correct outputs, no errors
2. **Diversity** - Cover all scenarios and edge cases
3. **Consistency** - Uniform format and style
4. **Balance** - Even distribution across categories
5. **Relevance** - Directly related to target task

### Minimum Dataset Sizes

| Task Complexity | Minimum Examples | Recommended | Optimal |
|----------------|------------------|-------------|---------|
| Simple classification | 50-100 | 500-1,000 | 5,000+ |
| Medium complexity | 200-500 | 1,000-2,000 | 10,000+ |
| Complex generation | 500-1,000 | 2,000-5,000 | 20,000+ |
| Domain expertise | 1,000-2,000 | 5,000-10,000 | 50,000+ |

---

## Dataset Size & Quality

### Quality > Quantity

**100 perfect examples > 1,000 mediocre examples**

Focus on:
- ✅ Accurate labels/outputs
- ✅ Representative of real-world use
- ✅ Clear, unambiguous examples
- ✅ Diverse scenarios
- ❌ Don't pad with low-quality data

### The 80/20 Rule

- **80% core cases** - Common scenarios
- **20% edge cases** - Unusual but important scenarios

---

## Task-Specific Guides

### Cypher Query Syntax Training

**Goal**: Train model to generate correct Cypher queries from natural language

#### Data Structure

```jsonl
{"instruction": "Convert to Cypher", "input": "Find all users named John", "output": "MATCH (u:User {name: 'John'}) RETURN u"}
{"instruction": "Convert to Cypher", "input": "Create a person named Alice with age 30", "output": "CREATE (p:Person {name: 'Alice', age: 30})"}
```

#### Coverage Checklist

**Basic Patterns** (30% of dataset):
```jsonl
{"instruction": "Convert to Cypher", "input": "Find all Person nodes", "output": "MATCH (p:Person) RETURN p"}
{"instruction": "Convert to Cypher", "input": "Find node with id 123", "output": "MATCH (n) WHERE id(n) = 123 RETURN n"}
{"instruction": "Convert to Cypher", "input": "Count all users", "output": "MATCH (u:User) RETURN count(u)"}
```

**Relationships** (25% of dataset):
```jsonl
{"instruction": "Convert to Cypher", "input": "Find who Alice knows", "output": "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(friend) RETURN friend"}
{"instruction": "Convert to Cypher", "input": "Create friendship from Alice to Bob", "output": "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)"}
{"instruction": "Convert to Cypher", "input": "Find friends of friends", "output": "MATCH (p:Person {name: 'Alice'})-[:KNOWS]->()-[:KNOWS]->(fof) RETURN DISTINCT fof"}
```

**Filtering** (20% of dataset):
```jsonl
{"instruction": "Convert to Cypher", "input": "Find users older than 25", "output": "MATCH (u:User) WHERE u.age > 25 RETURN u"}
{"instruction": "Convert to Cypher", "input": "Find people whose name starts with A", "output": "MATCH (p:Person) WHERE p.name STARTS WITH 'A' RETURN p"}
{"instruction": "Convert to Cypher", "input": "Find users with email containing gmail", "output": "MATCH (u:User) WHERE u.email CONTAINS 'gmail' RETURN u"}
```

**Aggregation** (15% of dataset):
```jsonl
{"instruction": "Convert to Cypher", "input": "Count users by country", "output": "MATCH (u:User) RETURN u.country, count(u) as count"}
{"instruction": "Convert to Cypher", "input": "Find average age of users", "output": "MATCH (u:User) RETURN avg(u.age) as average_age"}
{"instruction": "Convert to Cypher", "input": "Find person with most connections", "output": "MATCH (p:Person)-[r:KNOWS]->() RETURN p, count(r) as connections ORDER BY connections DESC LIMIT 1"}
```

**Complex Queries** (10% of dataset):
```jsonl
{"instruction": "Convert to Cypher", "input": "Find shortest path from Alice to Bob", "output": "MATCH path = shortestPath((a:Person {name: 'Alice'})-[*]-(b:Person {name: 'Bob'})) RETURN path"}
{"instruction": "Convert to Cypher", "input": "Recommend products for user based on similar users", "output": "MATCH (u:User {id: 123})-[:PURCHASED]->(p:Product)<-[:PURCHASED]-(similar:User)-[:PURCHASED]->(rec:Product) WHERE NOT (u)-[:PURCHASED]->(rec) RETURN rec, count(*) as score ORDER BY score DESC LIMIT 10"}
```

#### Data Generation Script

```python
import json
import random

# Templates for generating variations
templates = {
    "find_all": [
        ("Find all {label} nodes", "MATCH (n:{label}) RETURN n"),
        ("Get all {label}", "MATCH (n:{label}) RETURN n"),
        ("Show me all {label}", "MATCH (n:{label}) RETURN n"),
    ],
    "find_with_property": [
        ("Find {label} where {prop} is {value}", "MATCH (n:{label} {{{prop}: '{value}'}}) RETURN n"),
        ("Get {label} with {prop} equal to {value}", "MATCH (n:{label}) WHERE n.{prop} = '{value}' RETURN n"),
    ],
    "count": [
        ("Count all {label}", "MATCH (n:{label}) RETURN count(n)"),
        ("How many {label} are there", "MATCH (n:{label}) RETURN count(n) as total"),
    ],
}

labels = ["Person", "User", "Product", "Company", "Post", "Comment"]
properties = ["name", "email", "title", "status", "age", "created"]
values = ["John", "Active", "Premium", "test@example.com", "30", "2024"]

examples = []
for template_type, variants in templates.items():
    for variant in variants:
        for _ in range(5):  # Generate 5 variations each
            label = random.choice(labels)
            prop = random.choice(properties)
            value = random.choice(values)
            
            input_text = variant[0].format(label=label, prop=prop, value=value)
            output_text = variant[1].format(label=label, prop=prop, value=value)
            
            examples.append({
                "instruction": "Convert this natural language query to Cypher",
                "input": input_text,
                "output": output_text
            })

# Save dataset
with open('data/cypher_training.jsonl', 'w') as f:
    for ex in examples:
        f.write(json.dumps(ex) + '\n')

print(f"Generated {len(examples)} Cypher training examples")
```

#### Quality Validation

```python
# Validate Cypher syntax
from py2neo import Graph

def validate_cypher(query):
    """Check if Cypher query is syntactically valid."""
    try:
        # Use EXPLAIN to validate without executing
        graph = Graph("bolt://localhost:7687")
        graph.run(f"EXPLAIN {query}")
        return True
    except Exception as e:
        print(f"Invalid query: {query}")
        print(f"Error: {e}")
        return False

# Validate all examples
valid = 0
with open('data/cypher_training.jsonl', 'r') as f:
    for line in f:
        example = json.loads(line)
        if validate_cypher(example['output']):
            valid += 1

print(f"Valid queries: {valid}")
```

---

### TLP Classification

**Goal**: Classify data sensitivity using Traffic Light Protocol

#### Coverage Strategy

| TLP Level | Percentage | Examples |
|-----------|-----------|----------|
| TLP:CLEAR | 25% | Public docs, blog posts, press releases |
| TLP:GREEN | 20% | Community info, partner data (limited) |
| TLP:AMBER | 30% | Internal data, customer info, PII |
| TLP:AMBER+STRICT | 15% | Confidential with named recipients |
| TLP:RED | 10% | Highly confidential, critical security |

#### Example Dataset

```jsonl
{"instruction": "Classify TLP", "input": "Public API documentation", "output": "TLP:CLEAR", "reasoning": "Intended for public consumption"}
{"instruction": "Classify TLP", "input": "Customer email addresses", "output": "TLP:AMBER", "reasoning": "PII requiring limited distribution"}
{"instruction": "Classify TLP", "input": "CEO salary information", "output": "TLP:RED", "reasoning": "Highly confidential, must not leave organization"}
{"instruction": "Classify TLP", "input": "Security vulnerability in production system", "output": "TLP:AMBER+STRICT", "reasoning": "Critical security issue, named recipients only"}
{"instruction": "Classify TLP", "input": "Company blog post draft", "output": "TLP:GREEN", "reasoning": "Internal until published, then TLP:CLEAR"}
```

**Pro tip**: Include `reasoning` field to help model learn *why* classifications are made.

---

### Code Generation

#### Structure

```jsonl
{"instruction": "Write a Python function", "input": "Calculate factorial recursively", "output": "def factorial(n):\n    if n <= 1:\n        return 1\n    return n * factorial(n-1)"}
```

#### Quality Requirements

- ✅ Syntactically correct code
- ✅ Follows language conventions
- ✅ Includes error handling (for complex examples)
- ✅ Comments for complex logic
- ✅ Test with actual interpreter/compiler

---

### Text Classification

#### Balanced Dataset Example

```python
# Generate balanced dataset
categories = {
    "technical": 200,
    "business": 200,
    "support": 200,
    "sales": 200,
}

for category, count in categories.items():
    examples = generate_examples(category, count)
    # Ensure exactly equal distribution
```

---

## Data Collection Methods

### 1. Manual Creation

**Best for**: High-quality, specialized tasks

**Process**:
1. Define clear criteria
2. Create examples
3. Peer review
4. Iterate based on model performance

**Time**: Slow but highest quality

### 2. Synthetic Generation

**Best for**: Scaling dataset size, covering variations

**Tools**:
- GPT-4 for generating examples
- Template-based generation
- Combination of manual + AI

**Example - Using GPT-4**:
```python
import openai

def generate_examples(task, count=100):
    prompt = f"""Generate {count} training examples for: {task}
    
    Format each as:
    {{"instruction": "...", "input": "...", "output": "..."}}
    
    Ensure diversity and quality."""
    
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}]
    )
    
    return parse_examples(response.choices[0].message.content)
```

### 3. Real-World Collection

**Best for**: Production use cases, realistic scenarios

**Sources**:
- Support tickets
- User queries
- Application logs
- Expert annotations

**Important**: Anonymize PII, get consent

### 4. Hybrid Approach (Recommended)

1. **Manual core** (100-200 examples) - High quality foundation
2. **Synthetic expansion** (500-1000 examples) - Cover variations
3. **Real-world validation** (100+ examples) - Test on actual use
4. **Iterative refinement** - Add examples where model fails

---

## Quality Assurance

### Pre-Training Validation

```python
def validate_dataset(path):
    """Comprehensive dataset validation."""
    issues = []
    
    with open(path, 'r') as f:
        examples = [json.loads(line) for line in f]
    
    # 1. Format check
    for i, ex in enumerate(examples):
        required = ["instruction", "output"]
        missing = [k for k in required if k not in ex]
        if missing:
            issues.append(f"Line {i}: Missing fields {missing}")
    
    # 2. Duplicate check
    outputs = [ex["output"] for ex in examples]
    duplicates = len(outputs) - len(set(outputs))
    if duplicates > len(outputs) * 0.1:  # >10% duplicates
        issues.append(f"High duplicate rate: {duplicates} duplicates")
    
    # 3. Length check
    avg_input = sum(len(ex.get("input", "")) for ex in examples) / len(examples)
    avg_output = sum(len(ex["output"]) for ex in examples) / len(examples)
    if avg_output < 10:
        issues.append("Outputs seem too short")
    
    # 4. Balance check (if categories present)
    if "category" in examples[0]:
        from collections import Counter
        counts = Counter(ex["category"] for ex in examples)
        imbalance = max(counts.values()) / min(counts.values())
        if imbalance > 3:
            issues.append(f"Dataset imbalanced: {counts}")
    
    return issues

# Use it
issues = validate_dataset("data/training.jsonl")
if issues:
    print("⚠️ Dataset issues found:")
    for issue in issues:
        print(f"  - {issue}")
else:
    print("✅ Dataset validation passed")
```

### Post-Training Evaluation

```python
def evaluate_model_on_test_set(model, test_data):
    """Evaluate trained model."""
    correct = 0
    total = 0
    
    for example in test_data:
        prediction = model.generate(example["input"])
        if prediction.strip() == example["output"].strip():
            correct += 1
        total += 1
    
    accuracy = correct / total
    print(f"Accuracy: {accuracy:.2%}")
    
    return accuracy
```

---

## Common Pitfalls

### ❌ Pitfall 1: Insufficient Diversity

**Problem**: All examples too similar
```jsonl
{"input": "Find user named John", "output": "MATCH (u:User {name: 'John'}) RETURN u"}
{"input": "Find user named Jane", "output": "MATCH (u:User {name: 'Jane'}) RETURN u"}
{"input": "Find user named Bob", "output": "MATCH (u:User {name: 'Bob'}) RETURN u"}
```

**Solution**: Vary structure, not just values
```jsonl
{"input": "Find user named John", "output": "MATCH (u:User {name: 'John'}) RETURN u"}
{"input": "Get all users older than 25", "output": "MATCH (u:User) WHERE u.age > 25 RETURN u"}
{"input": "Count users in each city", "output": "MATCH (u:User) RETURN u.city, count(u) as count"}
```

### ❌ Pitfall 2: Data Leakage

**Problem**: Test examples too similar to training

**Solution**: 
- Completely separate test set
- Different scenarios, not just different values
- Hold out 10-20% of data *before* any analysis

### ❌ Pitfall 3: Ignoring Edge Cases

**Problem**: Only training on common cases

**Solution**: Deliberately include:
- Empty inputs
- Very long inputs
- Special characters
- Error cases
- Ambiguous queries

### ❌ Pitfall 4: Inconsistent Formatting

**Problem**: Mixed styles in outputs
```jsonl
{"output": "MATCH (n:User) RETURN n"}
{"output": "match (n:user) return n"}  // Different case
{"output": "MATCH(n:User)RETURN n"}    // No spaces
```

**Solution**: Enforce consistent style guide

### ❌ Pitfall 5: Too Few Examples Per Category

**Problem**: 5 examples of complex queries, 500 of simple

**Solution**: Minimum 20-50 examples per distinct pattern

---

## Quick Start Templates

### Cypher Query Training

```bash
# Download pre-made starter dataset
curl -o data/cypher_starter.jsonl https://raw.githubusercontent.com/orneryd/NornicDB/main/neural/data/examples/cypher_starter.jsonl

# Expand with your own patterns
python scripts/expand_cypher_dataset.py --input data/cypher_starter.jsonl --output data/cypher_full.jsonl --count 1000

# Train
python train.py --dataset data/cypher_full.jsonl --output_dir models/cypher-expert --epochs 5
```

### Classification Task

```python
# Template for classification tasks
categories = ["category_a", "category_b", "category_c"]

examples = []
for category in categories:
    for i in range(100):  # 100 per category
        # Generate or collect example
        examples.append({
            "instruction": f"Classify this text",
            "input": generate_text_for_category(category),
            "output": category
        })

# Shuffle to avoid category ordering bias
random.shuffle(examples)
```

---

## Resources

- **Cypher Documentation**: [Neo4j Cypher Manual](https://neo4j.com/docs/cypher-manual/current/)
- **Dataset Templates**: See `neural/data/examples/`
- **Validation Scripts**: See `neural/scripts/`
- **Community Datasets**: [HuggingFace Datasets](https://huggingface.co/datasets)

---

## Summary Checklist

Before training, ensure:

- [ ] Minimum dataset size met for task complexity
- [ ] All required fields present and valid
- [ ] Diverse examples covering main scenarios
- [ ] Edge cases included (10-20% of dataset)
- [ ] Consistent formatting throughout
- [ ] Balanced distribution (if classification)
- [ ] No data leakage between train/test
- [ ] Quality validated (syntax check, peer review)
- [ ] Test set reserved (10-20% held out)
- [ ] Documentation of labeling criteria

**Remember**: Start small (100 quality examples), train, evaluate, then expand based on where model fails.
