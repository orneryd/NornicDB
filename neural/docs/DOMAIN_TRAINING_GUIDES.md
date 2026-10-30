# Domain-Specific LLM Training Guides

**Comprehensive guides for training specialized LLMs across different domains**

---

## Table of Contents

1. [Magic: The Gathering Judge](#magic-the-gathering-judge)
2. [Agentic Coding Model](#agentic-coding-model)
3. [Conversational Assistant](#conversational-assistant)
4. [Code Repository Expert](#code-repository-expert)
5. [Technical Documentation QA](#technical-documentation-qa)
6. [Cypher Query Expert](#cypher-query-expert-enhanced)
7. [General Domain Adaptation](#general-domain-adaptation)

---

## Magic: The Gathering Judge

**Goal**: Train a model to act as a rules judge for MTG tournaments

### Phase 1: Data Collection

#### Sources
1. **Official Rules** (Primary - 100% accurate)
   - Comprehensive Rules Document (250+ pages)
   - MTG Judge Academy materials
   - Tournament Rules & Infractions Guide

2. **Card Interactions** (Examples)
   - Gatherer rulings database
   - Judge Tower scenarios
   - Known complex interactions

3. **Common Scenarios** (Training examples)
   - Stack interactions
   - Priority questions
   - Combat damage resolution
   - State-based actions

#### Dataset Structure

```jsonl
{"instruction": "Rule this MTG scenario", "input": "Player casts Lightning Bolt targeting opponent's creature. In response, opponent casts Giant Growth on that creature. What happens?", "output": "The stack resolves last-in-first-out. Giant Growth resolves first, giving the creature +3/+3 until end of turn. Then Lightning Bolt resolves, dealing 3 damage. If the creature's toughness after Giant Growth is greater than 3, it survives. If not, it dies. State-based actions are checked after the stack is empty.", "rule_reference": "CR 608.2, 704.3"}

{"instruction": "Explain MTG rule", "input": "When do state-based actions get checked?", "output": "State-based actions are checked whenever a player would receive priority. This happens: 1) After a spell or ability resolves, 2) After combat damage is dealt, 3) After each step or phase begins. The game checks for things like creatures with 0 or less toughness, players with 0 or less life, and applies them immediately.", "rule_reference": "CR 704"}

{"instruction": "Judge this interaction", "input": "Can I activate my creature's ability after it's been destroyed but before it goes to the graveyard?", "output": "No. Once a creature has been destroyed (either by lethal damage or a destroy effect), it cannot have its abilities activated. The creature is immediately moved to the graveyard as a state-based action. Triggered abilities that trigger on destruction will still trigger, but activated abilities cannot be used.", "rule_reference": "CR 112.6, 704.5f"}
```

### Phase 2: Data Generation Script

```python
#!/usr/bin/env python3
"""Generate MTG rules training data."""

import json
import random

class MTGRulesGenerator:
    def __init__(self):
        # Load comprehensive rules
        self.rules = self.load_rules("data/mtg/comprehensive_rules.txt")
        
        # Common cards for examples
        self.cards = {
            "removal": ["Lightning Bolt", "Path to Exile", "Fatal Push"],
            "creatures": ["Grizzly Bears", "Serra Angel", "Tarmogoyf"],
            "counterspells": ["Counterspell", "Mana Leak", "Force of Will"],
            "pump": ["Giant Growth", "Berserk", "Mutagenic Growth"]
        }
        
        # Scenario templates
        self.templates = {
            "stack": [
                ("Player A casts {spell}. Player B responds with {response}. What happens?",
                 "The stack resolves last-in-first-out. {response} resolves first, then {spell}. {outcome}")
            ],
            "combat": [
                ("A 3/3 creature attacks, and a 2/2 creature blocks. What happens?",
                 "Both creatures deal damage equal to their power to each other simultaneously. The 2/2 dies (2 toughness, 3 damage), the 3/3 survives with 2 damage marked (3 toughness, 2 damage).")
            ],
            "priority": [
                ("When does a player receive priority?",
                 "A player receives priority at the beginning of each step/phase (except untap and cleanup), after a spell/ability resolves, and after state-based actions are checked.")
            ]
        }
    
    def load_rules(self, path):
        """Load and parse comprehensive rules."""
        # Parse the official rules document
        # Extract rule numbers and text
        pass
    
    def generate_stack_interactions(self, count=100):
        """Generate stack interaction examples."""
        examples = []
        for _ in range(count):
            spell = random.choice(self.cards["removal"])
            response = random.choice(self.cards["pump"] + self.cards["counterspells"])
            
            # Generate outcome based on interaction
            if response in self.cards["counterspells"]:
                outcome = f"{spell} is countered and goes to the graveyard. The original target is unaffected."
            else:
                outcome = f"The target gets boosted before {spell} resolves. Calculate final damage/effect accordingly."
            
            examples.append({
                "instruction": "Rule this MTG scenario",
                "input": f"Player A casts {spell}. Player B responds with {response}. What happens?",
                "output": f"The stack resolves last-in-first-out (LIFO). {response} resolves first, then {spell}. {outcome}",
                "rule_reference": "CR 608.2"
            })
        
        return examples
    
    def generate_rule_explanations(self, count=200):
        """Generate rule explanation examples from comprehensive rules."""
        examples = []
        # Parse each rule and create Q&A pairs
        for rule_num, rule_text in self.rules.items():
            examples.append({
                "instruction": "Explain this MTG rule",
                "input": f"What is rule {rule_num}?",
                "output": rule_text,
                "rule_reference": rule_num
            })
        
        return examples
    
    def generate_tournament_scenarios(self, count=50):
        """Generate tournament procedure examples."""
        examples = [
            {
                "instruction": "Handle this tournament situation",
                "input": "A player drew 8 cards instead of 7 at the start of the game. What is the penalty?",
                "output": "This is a Game Rule Violation (GRV) for drawing extra cards. At Regular REL, this is typically a Warning. The Head Judge should be called to issue the appropriate fix (usually having the player shuffle the extra card back and continue). At Competitive REL, this is a Game Loss if not caught immediately.",
                "rule_reference": "IPG 2.3"
            },
            # Add more tournament scenarios
        ]
        return examples

# Generate dataset
generator = MTGRulesGenerator()
dataset = []
dataset.extend(generator.generate_stack_interactions(100))
dataset.extend(generator.generate_rule_explanations(200))
dataset.extend(generator.generate_tournament_scenarios(50))

# Save
with open('data/mtg_judge_training.jsonl', 'w') as f:
    for example in dataset:
        f.write(json.dumps(example) + '\n')
```

### Phase 3: Training

```bash
# 1. Collect MTG comprehensive rules
curl -o data/mtg/comprehensive_rules.txt "https://magic.wizards.com/en/rules"

# 2. Generate training data
python scripts/generate_mtg_dataset.py --count 5000 --output data/mtg_judge.jsonl

# 3. Validate
python scripts/validate_dataset.py data/mtg_judge.jsonl

# 4. Train (use larger model for complex rules)
python train.py \
  --preset phi3_mini \
  --dataset data/mtg_judge.jsonl \
  --output_dir models/mtg-judge \
  --epochs 10 \
  --learning_rate 1e-4

# 5. Export
python export_to_gguf.py \
  --model_dir models/mtg-judge \
  --output mtg-judge-q4.gguf
```

### Phase 4: Evaluation

```python
# Test with known scenarios
test_cases = [
    {
        "input": "Can I cast Lightning Bolt in response to my opponent casting Counterspell?",
        "expected_category": "stack_timing"
    },
    {
        "input": "What happens when two creatures with deathtouch fight?",
        "expected_category": "combat_damage"
    }
]

# Evaluate model responses
accuracy = evaluate_mtg_judge(model, test_cases)
```

### Key Considerations

- **Accuracy is critical** - Wrong rulings affect tournaments
- **Include rule citations** - Model should reference specific rules
- **Cover edge cases** - Complex interactions are most important
- **Update regularly** - Rules change with new sets

---

## Agentic Coding Model

**Goal**: Train a model that can write, debug, and modify code autonomously

### Phase 1: Data Architecture

#### Dataset Components

1. **Code Generation** (40%)
   - Function implementations from docstrings
   - Class implementations from specifications
   - Algorithm implementations

2. **Bug Fixing** (30%)
   - Broken code → Fixed code
   - Error messages → Solutions
   - Code smells → Refactored code

3. **Code Understanding** (20%)
   - Explain what code does
   - Identify patterns and anti-patterns
   - Suggest improvements

4. **Test Generation** (10%)
   - Code → Unit tests
   - Edge cases identification
   - Test coverage improvement

#### Dataset Structure

```jsonl
{"instruction": "Implement this function", "input": "def calculate_fibonacci(n: int) -> int:\n    \"\"\"Calculate the nth Fibonacci number using dynamic programming.\"\"\"\n    pass", "output": "def calculate_fibonacci(n: int) -> int:\n    \"\"\"Calculate the nth Fibonacci number using dynamic programming.\"\"\"\n    if n <= 1:\n        return n\n    \n    dp = [0] * (n + 1)\n    dp[1] = 1\n    \n    for i in range(2, n + 1):\n        dp[i] = dp[i-1] + dp[i-2]\n    \n    return dp[n]", "language": "python", "category": "generation"}

{"instruction": "Fix this buggy code", "input": "def binary_search(arr, target):\n    left, right = 0, len(arr)\n    while left < right:\n        mid = (left + right) // 2\n        if arr[mid] == target:\n            return mid\n        elif arr[mid] < target:\n            left = mid\n        else:\n            right = mid\n    return -1\n\nError: Infinite loop on certain inputs", "output": "def binary_search(arr, target):\n    left, right = 0, len(arr) - 1  # Fix 1: right should be len(arr) - 1\n    while left <= right:  # Fix 2: should be <= not <\n        mid = (left + right) // 2\n        if arr[mid] == target:\n            return mid\n        elif arr[mid] < target:\n            left = mid + 1  # Fix 3: increment to avoid infinite loop\n        else:\n            right = mid - 1  # Fix 4: decrement to avoid infinite loop\n    return -1\n\n# Fixes: 1) Correct initial right bound, 2) Include right in search, 3-4) Update bounds to avoid infinite loop", "language": "python", "category": "bug_fix"}

{"instruction": "Explain this code", "input": "def quick_select(arr, k):\n    pivot = arr[len(arr) // 2]\n    left = [x for x in arr if x < pivot]\n    mid = [x for x in arr if x == pivot]\n    right = [x for x in arr if x > pivot]\n    \n    if k < len(left):\n        return quick_select(left, k)\n    elif k < len(left) + len(mid):\n        return mid[0]\n    else:\n        return quick_select(right, k - len(left) - len(mid))", "output": "This implements the QuickSelect algorithm to find the kth smallest element in O(n) average time. It works by:\n\n1. Choose a pivot (middle element)\n2. Partition array into three parts: less than, equal to, and greater than pivot\n3. Recursively search the appropriate partition based on k's position\n\nThe algorithm is similar to QuickSort but only recurses on one partition, making it more efficient for finding a single element.", "language": "python", "category": "explanation"}

{"instruction": "Generate tests for this function", "input": "def is_palindrome(s: str) -> bool:\n    \"\"\"Check if string is a palindrome, ignoring spaces and case.\"\"\"\n    clean = ''.join(s.lower().split())\n    return clean == clean[::-1]", "output": "import pytest\n\ndef test_is_palindrome():\n    # Basic palindromes\n    assert is_palindrome(\"racecar\") == True\n    assert is_palindrome(\"noon\") == True\n    \n    # With spaces\n    assert is_palindrome(\"race car\") == True\n    assert is_palindrome(\"A man a plan a canal Panama\") == True\n    \n    # Case insensitive\n    assert is_palindrome(\"RaceCar\") == True\n    \n    # Not palindromes\n    assert is_palindrome(\"hello\") == False\n    assert is_palindrome(\"world\") == False\n    \n    # Edge cases\n    assert is_palindrome(\"\") == True\n    assert is_palindrome(\"a\") == True\n    assert is_palindrome(\"ab\") == False", "language": "python", "category": "test_generation"}
```

### Phase 2: Data Collection Script

```python
#!/usr/bin/env python3
"""Generate coding training data from repositories and problems."""

import os
import ast
import json
from pathlib import Path
from typing import List, Dict

class CodingDatasetGenerator:
    def __init__(self):
        self.languages = ["python", "go", "javascript", "java"]
    
    def extract_functions_from_repo(self, repo_path: str, language: str) -> List[Dict]:
        """Extract functions from codebase for training."""
        examples = []
        
        if language == "python":
            for py_file in Path(repo_path).rglob("*.py"):
                try:
                    with open(py_file, 'r') as f:
                        tree = ast.parse(f.read())
                    
                    for node in ast.walk(tree):
                        if isinstance(node, ast.FunctionDef):
                            # Extract function with docstring as input
                            docstring = ast.get_docstring(node)
                            if docstring:
                                # Create prompt: docstring → implementation
                                func_stub = self.create_function_stub(node, docstring)
                                func_impl = ast.unparse(node)
                                
                                examples.append({
                                    "instruction": "Implement this function",
                                    "input": func_stub,
                                    "output": func_impl,
                                    "language": "python",
                                    "category": "generation"
                                })
                except:
                    pass
        
        return examples
    
    def create_function_stub(self, func_node, docstring):
        """Create function stub with just signature and docstring."""
        args = ast.unparse(func_node.args)
        return_annotation = ""
        if func_node.returns:
            return_annotation = f" -> {ast.unparse(func_node.returns)}"
        
        return f"def {func_node.name}({args}){return_annotation}:\n    \"\"\"{docstring}\"\"\"\n    pass"
    
    def generate_bug_fixing_examples(self, count=100) -> List[Dict]:
        """Generate common bug patterns and fixes."""
        bug_patterns = [
            {
                "buggy": "for i in range(len(arr)):\n    if arr[i] == target:\n        break\nreturn i",
                "fixed": "for i in range(len(arr)):\n    if arr[i] == target:\n        return i\nreturn -1  # Not found",
                "explanation": "Variable 'i' undefined if loop completes without break"
            },
            {
                "buggy": "def divide(a, b):\n    return a / b",
                "fixed": "def divide(a, b):\n    if b == 0:\n        raise ValueError(\"Cannot divide by zero\")\n    return a / b",
                "explanation": "Missing zero division check"
            },
            # Add more patterns
        ]
        
        examples = []
        for pattern in bug_patterns:
            examples.append({
                "instruction": "Fix this buggy code",
                "input": f"{pattern['buggy']}\n\nError: {pattern['explanation']}",
                "output": f"{pattern['fixed']}\n\n# Fix: {pattern['explanation']}",
                "language": "python",
                "category": "bug_fix"
            })
        
        return examples
    
    def generate_from_leetcode_style(self, count=500) -> List[Dict]:
        """Generate algorithm implementation examples."""
        # Scrape or use existing algorithm problems
        # Create docstring → implementation pairs
        pass

# Usage
generator = CodingDatasetGenerator()

# Extract from existing codebases
nornicdb_examples = generator.extract_functions_from_repo("/path/to/NornicDB", "python")
bug_examples = generator.generate_bug_fixing_examples(100)
algorithm_examples = generator.generate_from_leetcode_style(500)

# Combine
all_examples = nornicdb_examples + bug_examples + algorithm_examples

with open('data/coding_training.jsonl', 'w') as f:
    for ex in all_examples:
        f.write(json.dumps(ex) + '\n')
```

### Phase 3: Repository-Specific Training

Train model on specific codebase (e.g., NornicDB):

```bash
# 1. Extract patterns from repository
python scripts/extract_repo_patterns.py \
  --repo /Users/timothysweet/src/NornicDB \
  --language go \
  --output data/nornicdb_patterns.jsonl

# 2. Add general coding examples
python scripts/generate_coding_dataset.py \
  --count 2000 \
  --output data/coding_general.jsonl

# 3. Combine datasets
cat data/nornicdb_patterns.jsonl data/coding_general.jsonl > data/coding_full.jsonl

# 4. Train
python train.py \
  --preset qwen_1_5b \
  --dataset data/coding_full.jsonl \
  --output_dir models/coding-agent \
  --epochs 5 \
  --learning_rate 2e-4

# 5. Export
python export_to_gguf.py \
  --model_dir models/coding-agent \
  --output coding-agent-q4.gguf
```

### Key Considerations

- **Syntax correctness** - All generated code must be valid
- **Idiomatic code** - Follow language conventions
- **Error handling** - Include edge cases and validation
- **Comments** - Explain complex logic
- **Testing** - Validate with actual compilers/interpreters

---

## Conversational Assistant

**Goal**: Build a helpful, engaging conversational AI

### Data Structure

```jsonl
{"messages": [{"role": "system", "content": "You are a helpful, friendly assistant."}, {"role": "user", "content": "How do I sort a list in Python?"}, {"role": "assistant", "content": "There are several ways to sort a list in Python:\n\n1. Using `sort()` method (in-place):\n```python\nmy_list = [3, 1, 4, 1, 5]\nmy_list.sort()\nprint(my_list)  # [1, 1, 3, 4, 5]\n```\n\n2. Using `sorted()` function (returns new list):\n```python\nmy_list = [3, 1, 4, 1, 5]\nsorted_list = sorted(my_list)\n```\n\n3. Reverse order:\n```python\nmy_list.sort(reverse=True)\n```\n\nWhich method you use depends on whether you want to modify the original list or create a new one!"}]}

{"messages": [{"role": "system", "content": "You are a helpful, friendly assistant."}, {"role": "user", "content": "I'm feeling overwhelmed with work. Any advice?"}, {"role": "assistant", "content": "I understand feeling overwhelmed - it happens to everyone! Here are some strategies that might help:\n\n1. **Break it down**: Split large tasks into smaller, manageable steps\n2. **Prioritize**: Focus on what's truly urgent and important\n3. **Take breaks**: Short breaks can actually improve productivity\n4. **Ask for help**: Don't hesitate to delegate or request assistance\n5. **One thing at a time**: Multitasking often increases stress\n\nRemember, it's okay to feel this way. What specific aspect is most overwhelming right now? Maybe I can help you think through it."}]}
```

### Training

```bash
# 1. Collect conversational data
# - ShareGPT format conversations
# - Customer support logs (anonymized)
# - Q&A forums (Stack Overflow, Reddit)

# 2. Format as chat messages
python scripts/format_conversational_data.py \
  --input data/raw_conversations/ \
  --output data/conversations.jsonl

# 3. Train
python train.py \
  --preset qwen_1_5b \
  --dataset data/conversations.jsonl \
  --output_dir models/assistant \
  --epochs 3

# 4. Export
python export_to_gguf.py \
  --model_dir models/assistant \
  --output assistant-q4.gguf
```

---

## Code Repository Expert

**Goal**: Train model that understands a specific codebase (like NornicDB)

### Phase 1: Repository Analysis

```bash
# Extract all code patterns
python scripts/analyze_repository.py \
  --repo /Users/timothysweet/src/NornicDB \
  --output data/repo_analysis.json
```

### Phase 2: Generate Training Data

```python
#!/usr/bin/env python3
"""Extract training data from code repository."""

import os
import json
from pathlib import Path
from tree_sitter import Language, Parser
import subprocess

class RepositoryDataExtractor:
    def __init__(self, repo_path: str, language: str = "go"):
        self.repo_path = Path(repo_path)
        self.language = language
        self.examples = []
    
    def extract_all_patterns(self):
        """Extract various patterns from repository."""
        self.extract_function_implementations()
        self.extract_type_definitions()
        self.extract_api_usage_patterns()
        self.extract_test_patterns()
        self.extract_error_handling()
        self.extract_documentation()
    
    def extract_function_implementations(self):
        """Extract function signatures → implementations."""
        for go_file in self.repo_path.rglob("*.go"):
            try:
                with open(go_file, 'r') as f:
                    content = f.read()
                
                # Parse Go code (would need tree-sitter-go)
                functions = self.parse_go_functions(content)
                
                for func in functions:
                    # Create examples
                    self.examples.append({
                        "instruction": "Implement this function based on NornicDB patterns",
                        "input": func['signature'] + "\n" + func['doc'],
                        "output": func['implementation'],
                        "file": str(go_file.relative_to(self.repo_path)),
                        "category": "implementation"
                    })
            except:
                pass
    
    def extract_api_usage_patterns(self):
        """Extract how APIs are used in the codebase."""
        # Find common patterns like:
        # - How storage.Engine is used
        # - How cypher.Executor is initialized
        # - Common error handling patterns
        pass
    
    def extract_test_patterns(self):
        """Extract test → implementation relationships."""
        test_files = list(self.repo_path.rglob("*_test.go"))
        
        for test_file in test_files:
            impl_file = str(test_file).replace("_test.go", ".go")
            if os.path.exists(impl_file):
                # Create test ↔ implementation pairs
                pass
    
    def extract_documentation(self):
        """Extract code → documentation examples."""
        # Package docs, function docs, inline comments
        pass
    
    def generate_qa_pairs(self):
        """Generate Q&A about the codebase."""
        examples = [
            {
                "instruction": "Answer this question about NornicDB",
                "input": "How do I create a new node in NornicDB?",
                "output": "To create a new node in NornicDB:\n\n```go\nnode := &storage.Node{\n    ID: storage.NewNodeID(),\n    Labels: []string{\"Person\"},\n    Properties: map[string]interface{}{\n        \"name\": \"Alice\",\n        \"age\": 30,\n    },\n}\n\nerr := engine.CreateNode(node)\nif err != nil {\n    return err\n}\n```\n\nThe storage.Engine interface provides the CreateNode method. You need to create a Node struct with an ID, labels, and properties."
            },
            {
                "instruction": "Answer this question about NornicDB",
                "input": "What's the difference between MemoryEngine and BadgerEngine?",
                "output": "MemoryEngine and BadgerEngine are two storage implementations in NornicDB:\n\n**MemoryEngine**: In-memory storage, fast but not persistent. Used for testing and development.\n\n**BadgerEngine**: Persistent storage using BadgerDB (LSM-tree based). Used for production with data durability.\n\nBoth implement the storage.Engine interface, so they can be swapped without code changes."
            }
        ]
        return examples

# Generate dataset
extractor = RepositoryDataExtractor("/Users/timothysweet/src/NornicDB")
extractor.extract_all_patterns()
qa_pairs = extractor.generate_qa_pairs()

all_data = extractor.examples + qa_pairs

with open('data/nornicdb_expert.jsonl', 'w') as f:
    for example in all_data:
        f.write(json.dumps(example) + '\n')
```

### Phase 3: Training

```bash
# Train on repository
python train.py \
  --preset qwen_1_5b \
  --dataset data/nornicdb_expert.jsonl \
  --output_dir models/nornicdb-expert \
  --epochs 10  # More epochs for deep knowledge

# Export
python export_to_gguf.py \
  --model_dir models/nornicdb-expert \
  --output nornicdb-expert-q4.gguf
```

---

## General Domain Adaptation Recipe

### 1. Data Collection (1000-10,000 examples)
- **Expert knowledge**: Official docs, papers, books
- **Examples**: Real-world cases, worked solutions
- **Conversations**: Q&A forums, support tickets
- **Code**: If applicable, actual implementations

### 2. Data Structuring
Choose format based on task:
- **Instructions**: Classification, generation tasks
- **Chat**: Conversational, multi-turn
- **Completion**: Code completion, text generation

### 3. Quality Assurance
```bash
python scripts/validate_dataset.py data/domain.jsonl
```

### 4. Training
```bash
python train.py --preset qwen_1_5b --dataset data/domain.jsonl --output_dir models/domain-expert
```

### 5. Evaluation
- Test on held-out examples
- Domain expert review
- A/B testing vs baseline

### 6. Export & Deploy
```bash
python export_to_gguf.py --model_dir models/domain-expert --output domain-expert-q4.gguf
cp domain-expert-q4.gguf /data/models/
```

---

## English Language Grounding

**Important**: Ensure model understands English even when specialized

### Strategy

1. **Mix general English** (20-30% of dataset)
   - Common sense reasoning
   - General knowledge Q&A
   - Basic instructions

2. **Domain + English together** (70-80%)
   - Domain examples with clear explanations
   - Natural language descriptions
   - Context and reasoning

### Example Dataset Mix

```jsonl
# 70% Domain-specific
{"instruction": "Generate Cypher", "input": "Find users", "output": "MATCH (u:User) RETURN u"}

# 20% General English
{"instruction": "Answer this question", "input": "What is the capital of France?", "output": "The capital of France is Paris."}

# 10% Domain + Explanation
{"instruction": "Explain this Cypher query", "input": "MATCH (n)-[:KNOWS]->(m) RETURN n, m", "output": "This Cypher query finds all pairs of nodes where one 'knows' the other. MATCH finds patterns, (n) and (m) are node variables, [:KNOWS] is the relationship type, and -> indicates direction. RETURN outputs the matched nodes."}
```

### Training with English Grounding

```bash
# 1. Generate domain data
python scripts/generate_cypher_dataset.py --count 1400 --output data/cypher.jsonl

# 2. Add general English examples (600 examples)
cat data/general_english_qa.jsonl >> data/cypher.jsonl

# 3. Train (total 2000 examples, 70/30 split)
python train.py \
  --preset qwen_1_5b \
  --dataset data/cypher.jsonl \
  --output_dir models/cypher-expert \
  --epochs 5
```

---

## Summary Workflow

For ANY domain:

1. **Define scope** - What should the model do?
2. **Collect data** - 1000-10,000 quality examples
3. **Structure data** - Choose appropriate format
4. **Add English grounding** - 20-30% general examples
5. **Validate** - Check quality before training
6. **Train** - Use appropriate preset for complexity
7. **Evaluate** - Test on held-out examples
8. **Export** - Convert to GGUF
9. **Deploy** - Use in NornicDB or llama.cpp
10. **Iterate** - Improve based on failures

See next doc for specific implementation scripts →
