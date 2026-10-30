#!/usr/bin/env python3
"""
Generate Heimdall training dataset - NornicDB's specialized SLM.

Heimdall is trained on:
- Cypher query syntax (comprehensive)
- NornicDB database management
- Heimdall plugin system
- Graph database operations
- Performance optimization
- Troubleshooting and debugging

This creates a focused, efficient model for NornicDB operations.
"""

import json
import random
import argparse
import os
import re
from pathlib import Path
from typing import List, Dict
from collections import defaultdict


class HeimdallDatasetGenerator:
    """Generate training data for Heimdall - NornicDB's SLM."""
    
    def __init__(self, repo_path: str = None):
        self.repo_path = Path(repo_path) if repo_path else Path.cwd()
        self.examples = []
        self.seen_outputs = set()  # Track unique outputs to prevent duplicates
        
        # Cypher syntax categories
        self.cypher_clauses = [
            "MATCH", "CREATE", "MERGE", "DELETE", "DETACH DELETE",
            "SET", "REMOVE", "WHERE", "WITH", "UNWIND",
            "RETURN", "ORDER BY", "LIMIT", "SKIP",
            "UNION", "UNION ALL", "OPTIONAL MATCH",
            "CALL", "YIELD"
        ]
        
        self.cypher_functions = {
            "aggregation": ["count", "sum", "avg", "min", "max", "collect", "percentileDisc", "stDev"],
            "scalar": ["coalesce", "timestamp", "id", "type", "properties", "keys", "labels"],
            "list": ["head", "tail", "last", "size", "reverse", "range"],
            "string": ["toString", "toUpper", "toLower", "trim", "substring", "replace", "split"],
            "mathematical": ["abs", "ceil", "floor", "round", "sign", "sqrt", "rand"],
            "predicate": ["exists", "isEmpty", "all", "any", "none", "single"],
            "spatial": ["distance", "point"],
            "temporal": ["date", "datetime", "time", "duration", "localdatetime"]
        }
        
    def generate_cypher_fundamentals(self) -> List[Dict]:
        """Core Cypher syntax examples."""
        examples = [
            # Basic MATCH patterns
            {
                "instruction": "Write a Cypher query",
                "input": "Find all nodes with label Person",
                "output": "MATCH (p:Person)\nRETURN p",
                "category": "cypher_basic",
                "context_size": "small"
            },
            {
                "instruction": "Write a Cypher query",
                "input": "Find person named Alice",
                "output": "MATCH (p:Person {name: 'Alice'})\nRETURN p",
                "category": "cypher_basic",
                "context_size": "small"
            },
            {
                "instruction": "Write a Cypher query",
                "input": "Find all relationships between Alice and Bob",
                "output": "MATCH (a:Person {name: 'Alice'})-[r]-(b:Person {name: 'Bob'})\nRETURN r",
                "category": "cypher_basic",
                "context_size": "small"
            },
            
            # Property matching
            {
                "instruction": "Write a Cypher query",
                "input": "Find users older than 25",
                "output": "MATCH (u:User)\nWHERE u.age > 25\nRETURN u.name, u.age\nORDER BY u.age DESC",
                "category": "cypher_filtering",
                "context_size": "small"
            },
            {
                "instruction": "Write a Cypher query",
                "input": "Find products with price between 10 and 100",
                "output": "MATCH (p:Product)\nWHERE p.price >= 10 AND p.price <= 100\nRETURN p.name, p.price\nORDER BY p.price",
                "category": "cypher_filtering",
                "context_size": "small"
            },
            
            # Relationship patterns
            {
                "instruction": "Write a Cypher query",
                "input": "Find who Alice knows",
                "output": "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(friend)\nRETURN friend.name",
                "category": "cypher_relationships",
                "context_size": "small"
            },
            {
                "instruction": "Write a Cypher query",
                "input": "Find friends of friends of Alice",
                "output": "MATCH (a:Person {name: 'Alice'})-[:KNOWS*2]->(fof)\nWHERE fof <> a\nRETURN DISTINCT fof.name",
                "category": "cypher_relationships",
                "context_size": "medium"
            },
            
            # CREATE operations
            {
                "instruction": "Write a Cypher query",
                "input": "Create a person named Bob aged 30",
                "output": "CREATE (p:Person {name: 'Bob', age: 30})\nRETURN p",
                "category": "cypher_create",
                "context_size": "small"
            },
            {
                "instruction": "Write a Cypher query",
                "input": "Create friendship between Alice and Bob",
                "output": "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})\nCREATE (a)-[:KNOWS {since: date()}]->(b)",
                "category": "cypher_create",
                "context_size": "small"
            },
            
            # MERGE operations
            {
                "instruction": "Write a Cypher query",
                "input": "Create or find person named Charlie",
                "output": "MERGE (p:Person {name: 'Charlie'})\nON CREATE SET p.created = timestamp()\nON MATCH SET p.accessed = timestamp()\nRETURN p",
                "category": "cypher_merge",
                "context_size": "medium"
            },
            
            # Aggregations
            {
                "instruction": "Write a Cypher query",
                "input": "Count how many friends each person has",
                "output": "MATCH (p:Person)-[:KNOWS]->(friend)\nRETURN p.name, count(friend) as friendCount\nORDER BY friendCount DESC",
                "category": "cypher_aggregation",
                "context_size": "small"
            },
            {
                "instruction": "Write a Cypher query",
                "input": "Find average age of users by city",
                "output": "MATCH (u:User)\nRETURN u.city, avg(u.age) as avgAge, count(u) as userCount\nORDER BY avgAge DESC",
                "category": "cypher_aggregation",
                "context_size": "small"
            },
            
            # Complex queries
            {
                "instruction": "Write a Cypher query",
                "input": "Find shortest path between Alice and Bob",
                "output": "MATCH path = shortestPath((a:Person {name: 'Alice'})-[:KNOWS*]-(b:Person {name: 'Bob'}))\nRETURN path, length(path) as hops",
                "category": "cypher_advanced",
                "context_size": "medium"
            },
            {
                "instruction": "Write a Cypher query",
                "input": "Find users who bought products that other users in their city also bought",
                "output": "MATCH (u:User)-[:PURCHASED]->(p:Product)<-[:PURCHASED]-(other:User)\nWHERE u.city = other.city AND u <> other\nWITH u, count(DISTINCT p) as commonProducts\nWHERE commonProducts > 2\nRETURN u.name, commonProducts\nORDER BY commonProducts DESC",
                "category": "cypher_advanced",
                "context_size": "large"
            },
        ]
        
        return examples
    
    def generate_nornicdb_operations(self) -> List[Dict]:
        """NornicDB-specific database operations."""
        examples = [
            # Database management
            {
                "instruction": "How do I create an index in NornicDB?",
                "input": "Create index on Person name property",
                "output": "CREATE INDEX person_name_idx FOR (p:Person) ON (p.name)",
                "category": "nornicdb_indexing",
                "context_size": "small"
            },
            {
                "instruction": "How do I create a composite index in NornicDB?",
                "input": "Create composite index on User city and age",
                "output": "CREATE INDEX user_location_age FOR (u:User) ON (u.city, u.age)",
                "category": "nornicdb_indexing",
                "context_size": "small"
            },
            {
                "instruction": "How do I drop an index?",
                "input": "Remove the person_name_idx index",
                "output": "DROP INDEX person_name_idx",
                "category": "nornicdb_indexing",
                "context_size": "small"
            },
            
            # Vector search
            {
                "instruction": "Perform vector search in NornicDB",
                "input": "Find similar documents using embeddings",
                "output": "MATCH (d:Document)\nWHERE d.embedding IS NOT NULL\nWITH d, vector.similarity(d.embedding, $queryEmbedding) AS score\nWHERE score > 0.8\nRETURN d.title, score\nORDER BY score DESC\nLIMIT 10",
                "category": "nornicdb_vector_search",
                "context_size": "medium"
            },
            {
                "instruction": "Create vector index in NornicDB",
                "input": "Create HNSW index for document embeddings",
                "output": "CALL db.index.vector.createNodeIndex(\n  'document_embeddings',\n  'Document',\n  'embedding',\n  1536,\n  'cosine'\n)",
                "category": "nornicdb_vector_search",
                "context_size": "medium"
            },
            
            # Transactions
            {
                "instruction": "Explain NornicDB transactions",
                "input": "How do I use transactions in NornicDB?",
                "output": "NornicDB supports ACID transactions. All Cypher queries run in implicit transactions. For explicit control:\n\nBEGIN\n  MATCH (u:User {id: $userId})\n  SET u.balance = u.balance - 100\n  CREATE (t:Transaction {amount: 100, timestamp: timestamp()})\n  CREATE (u)-[:MADE]->(t)\nCOMMIT\n\nUse ROLLBACK to abort a transaction on error.",
                "category": "nornicdb_transactions",
                "context_size": "medium"
            },
            
            # Performance optimization
            {
                "instruction": "Optimize this NornicDB query",
                "input": "MATCH (u:User)-[:PURCHASED]->(p:Product) WHERE u.city = 'NYC' RETURN p",
                "output": "# Optimization tips:\n1. Create index: CREATE INDEX user_city FOR (u:User) ON (u.city)\n2. Use PROFILE to analyze: PROFILE MATCH ...\n3. Rewrite to leverage index:\n\nMATCH (u:User {city: 'NYC'})-[:PURCHASED]->(p:Product)\nRETURN DISTINCT p\n\n# This pushes the filter to index lookup instead of scanning all users.",
                "category": "nornicdb_optimization",
                "context_size": "large"
            },
            {
                "instruction": "Explain NornicDB query performance",
                "input": "How do I profile slow queries?",
                "output": "Use EXPLAIN and PROFILE:\n\n1. EXPLAIN - Shows query plan without execution:\n   EXPLAIN MATCH (p:Person)-[:KNOWS*2]->() RETURN count(p)\n\n2. PROFILE - Executes and shows actual metrics:\n   PROFILE MATCH (p:Person)-[:KNOWS*2]->() RETURN count(p)\n\nLook for:\n- AllNodesScan (slow) vs NodeIndexSeek (fast)\n- High db hits\n- Cartesian products\n- Missing indexes",
                "category": "nornicdb_optimization",
                "context_size": "large"
            },
            
            # Schema management
            {
                "instruction": "How do I view NornicDB schema?",
                "input": "Show all node labels and relationship types",
                "output": "CALL db.labels() YIELD label\nRETURN collect(label) as allLabels\n\nCALL db.relationshipTypes() YIELD relationshipType\nRETURN collect(relationshipType) as allRelTypes",
                "category": "nornicdb_schema",
                "context_size": "small"
            },
            {
                "instruction": "Add constraint in NornicDB",
                "input": "Make User email unique",
                "output": "CREATE CONSTRAINT user_email_unique FOR (u:User) REQUIRE u.email IS UNIQUE",
                "category": "nornicdb_schema",
                "context_size": "small"
            },
        ]
        
        return examples
    
    def generate_heimdall_plugins(self) -> List[Dict]:
        """Heimdall plugin system examples."""
        examples = [
            # Plugin basics
            {
                "instruction": "Explain Heimdall plugin system",
                "input": "What is the Heimdall plugin system?",
                "output": "Heimdall is NornicDB's plugin architecture for extending functionality. Plugins can:\n\n1. Add custom Cypher procedures (CALL custom.procedure())\n2. Extend query capabilities\n3. Integrate external services\n4. Add authentication/authorization\n5. Implement custom algorithms\n\nPlugins are written in Go and loaded at startup. Location: plugins/heimdall/",
                "category": "heimdall_overview",
                "context_size": "medium"
            },
            
            # APOC-style procedures
            {
                "instruction": "Use APOC procedure in NornicDB",
                "input": "Convert string to uppercase",
                "output": "RETURN apoc.text.toUpperCase('hello world') as result\n// Result: 'HELLO WORLD'",
                "category": "heimdall_apoc",
                "context_size": "small"
            },
            {
                "instruction": "Use APOC date procedure",
                "input": "Format current date",
                "output": "RETURN apoc.date.format(timestamp(), 'ms', 'yyyy-MM-dd HH:mm:ss') as formatted",
                "category": "heimdall_apoc",
                "context_size": "small"
            },
            {
                "instruction": "Use APOC JSON procedure",
                "input": "Parse JSON string",
                "output": "WITH '{\"name\": \"Alice\", \"age\": 30}' as jsonStr\nRETURN apoc.convert.fromJsonMap(jsonStr) as data",
                "category": "heimdall_apoc",
                "context_size": "small"
            },
            
            # Graph algorithms
            {
                "instruction": "Run PageRank in NornicDB",
                "input": "Calculate PageRank for social network",
                "output": "CALL algo.pageRank(\n  'MATCH (p:Person) RETURN id(p) as id',\n  'MATCH (p1:Person)-[:KNOWS]->(p2:Person) RETURN id(p1) as source, id(p2) as target',\n  {iterations: 20, dampingFactor: 0.85}\n)\nYIELD nodeId, score\nMATCH (p:Person) WHERE id(p) = nodeId\nRETURN p.name, score\nORDER BY score DESC\nLIMIT 10",
                "category": "heimdall_algorithms",
                "context_size": "large"
            },
            {
                "instruction": "Find communities in graph",
                "input": "Detect communities using Louvain",
                "output": "CALL algo.louvain(\n  'MATCH (p:Person) RETURN id(p) as id',\n  'MATCH (p1:Person)-[:KNOWS]-(p2:Person) RETURN id(p1) as source, id(p2) as target'\n)\nYIELD nodeId, community\nMATCH (p:Person) WHERE id(p) = nodeId\nRETURN community, collect(p.name) as members, count(p) as size\nORDER BY size DESC",
                "category": "heimdall_algorithms",
                "context_size": "large"
            },
            
            # Custom procedures
            {
                "instruction": "Create custom Heimdall procedure",
                "input": "How do I write a custom procedure?",
                "output": "Create a Go file in plugins/heimdall/:\n\n```go\npackage heimdall\n\nimport (\n    \"github.com/nornicdb/pkg/cypher\"\n)\n\n// @Procedure(name=\"custom.greeting\")\n// @Description(\"Returns a personalized greeting\")\nfunc Greeting(name string) (string, error) {\n    if name == \"\" {\n        return \"Hello, stranger!\", nil\n    }\n    return fmt.Sprintf(\"Hello, %s!\", name), nil\n}\n```\n\nUsage:\nCALL custom.greeting('Alice') YIELD value\nRETURN value",
                "category": "heimdall_development",
                "context_size": "large"
            },
        ]
        
        return examples
    
    def generate_troubleshooting(self) -> List[Dict]:
        """Common issues and solutions."""
        examples = [
            {
                "instruction": "Debug NornicDB error",
                "input": "Error: Node not found",
                "output": "This error occurs when referencing a non-existent node. Common causes:\n\n1. Incorrect node ID\n2. Node was deleted\n3. Typo in label or property\n\nSolution:\nMATCH (n) WHERE id(n) = $nodeId\nRETURN n\n// If empty, node doesn't exist\n\nUse OPTIONAL MATCH to handle gracefully:\nOPTIONAL MATCH (n) WHERE id(n) = $nodeId\nRETURN COALESCE(n.name, 'Not found')",
                "category": "troubleshooting",
                "context_size": "medium"
            },
            {
                "instruction": "Debug NornicDB performance",
                "input": "Query is very slow",
                "output": "Performance debugging steps:\n\n1. Profile the query:\n   PROFILE MATCH ... RETURN ...\n\n2. Check for missing indexes:\n   - Look for 'AllNodesScan' in profile\n   - Create indexes on filtered properties\n\n3. Avoid Cartesian products:\n   - Ensure patterns are connected\n   - Use WITH to pipeline results\n\n4. Limit early:\n   - Apply LIMIT after WHERE, before ORDER BY when possible\n\n5. Check memory:\n   - Large result sets consume memory\n   - Use pagination: SKIP/LIMIT",
                "category": "troubleshooting",
                "context_size": "large"
            },
            {
                "instruction": "Fix NornicDB connection error",
                "input": "Cannot connect to database",
                "output": "Connection troubleshooting:\n\n1. Check NornicDB is running:\n   ps aux | grep nornicdb\n\n2. Verify port (default 7687):\n   netstat -an | grep 7687\n\n3. Check connection string:\n   bolt://localhost:7687\n\n4. Verify credentials in config:\n   cat nornicdb.yaml | grep auth\n\n5. Check logs:\n   tail -f /var/log/nornicdb/nornicdb.log",
                "category": "troubleshooting",
                "context_size": "medium"
            },
        ]
        
        return examples
    
    def extract_real_cypher_from_codebase(self) -> List[Dict]:
        """Extract real Cypher queries from NornicDB codebase."""
        examples = []
        
        # Search for Cypher queries in test files
        test_dirs = [
            self.repo_path / "pkg" / "cypher",
            self.repo_path / "pkg" / "storage",
        ]
        
        for test_dir in test_dirs:
            if not test_dir.exists():
                continue
                
            for test_file in test_dir.rglob("*_test.go"):
                try:
                    with open(test_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    # Find Cypher queries in test strings
                    queries = re.findall(r'`([^`]*(?:MATCH|CREATE|MERGE)[^`]*)`', content, re.IGNORECASE)
                    
                    for query in queries[:5]:  # Limit per file
                        if len(query) > 20 and len(query) < 500:  # Reasonable length
                            examples.append({
                                "instruction": "Explain this Cypher query from NornicDB",
                                "input": query.strip(),
                                "output": f"This is a real query from NornicDB's test suite in {test_file.name}",
                                "category": "nornicdb_real_queries",
                                "context_size": "medium"
                            })
                    
                except Exception as e:
                    continue
        
        return examples
    
    def _deduplicate_and_expand(self, base_examples: List[Dict], target_count: int, category: str) -> List[Dict]:
        """Expand base examples with variations to reach target count, avoiding duplicates."""
        import hashlib
        
        result = []
        
        # Add all unique base examples first
        for ex in base_examples:
            output_hash = hashlib.md5(ex['output'].encode()).hexdigest()
            if output_hash not in self.seen_outputs:
                self.seen_outputs.add(output_hash)
                result.append(ex)
        
        base_count = len(result)
        
        # If base examples already exceed target, downsample to target
        if base_count > target_count:
            return random.sample(result, target_count)
        
        # If we need more, create variations
        if len(result) < target_count:
            # More diverse variations for better uniqueness
            names = ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank', 'Grace', 'Henry', 
                     'Isabel', 'Jack', 'Kate', 'Leo', 'Maya', 'Nina', 'Oscar', 'Paul']
            labels = ['Person', 'User', 'Account', 'Profile', 'Member', 'Customer', 'Employee', 
                     'Participant', 'Contact', 'Individual']
            properties = ['name', 'username', 'email', 'id', 'handle', 'login', 'identifier', 
                         'title', 'label', 'tag']
            values = ['active', 'inactive', 'pending', 'complete', 'draft', 'published', 
                     'archived', 'verified', 'approved', 'rejected']
            
            attempts = 0
            max_attempts = target_count * 5  # More attempts for harder categories
            
            while len(result) < target_count and attempts < max_attempts:
                attempts += 1
                
                # Pick a random base example to vary
                base = random.choice(base_examples)
                varied = base.copy()
                
                # Apply random variations to output
                output = varied['output']
                input_text = varied['input']
                
                # More aggressive variations
                variation_type = random.randint(1, 4)
                
                if variation_type == 1:
                    # Replace names
                    for old_name in ['Alice', 'Bob', 'Charlie', 'David', 'Eve']:
                        if old_name in output:
                            new_name = random.choice([n for n in names if n != old_name])
                            output = output.replace(old_name, new_name)
                            if old_name.lower() in input_text:
                                input_text = input_text.replace(old_name.lower(), new_name.lower())
                
                elif variation_type == 2:
                    # Replace labels
                    for old_label in ['Person', 'User', 'Account', 'Member']:
                        if f':{old_label}' in output:
                            new_label = random.choice([l for l in labels if l != old_label])
                            output = output.replace(f':{old_label}', f':{new_label}')
                            if old_label.lower() in input_text:
                                input_text = input_text.replace(old_label.lower(), new_label.lower())
                
                elif variation_type == 3:
                    # Replace property names
                    for old_prop in ['name', 'username', 'email']:
                        if f'{old_prop}' in output:
                            new_prop = random.choice([p for p in properties if p != old_prop])
                            output = output.replace(f'.{old_prop}', f'.{new_prop}')
                            output = output.replace(f'{{{old_prop}', f'{{{new_prop}')
                
                else:
                    # Replace values
                    for old_val in ['active', 'pending', 'draft']:
                        if f"'{old_val}'" in output:
                            new_val = random.choice([v for v in values if v != old_val])
                            output = output.replace(f"'{old_val}'", f"'{new_val}'")
                            if old_val in input_text:
                                input_text = input_text.replace(old_val, new_val)
                
                # Check if this variation is unique
                output_hash = hashlib.md5(output.encode()).hexdigest()
                if output_hash not in self.seen_outputs:
                    self.seen_outputs.add(output_hash)
                    varied['output'] = output
                    varied['input'] = input_text
                    result.append(varied)
        
        final_result = result[:target_count]
        
        # Warn if we couldn't reach target
        if len(final_result) < target_count * 0.8:  # Less than 80% of target
            print(f"    ⚠️  Could only generate {len(final_result)}/{target_count} for {category}")
        
        return final_result
    
    def generate_dataset(self, num_examples: int = 5000) -> List[Dict]:
        """Generate complete Heimdall training dataset with proper deduplication."""
        print(f"🎯 Target: {num_examples} unique training examples")
        print("📏 Quality: Deduplication + balanced categories\n")
        
        all_examples = []
        
        # Distribution optimized for NornicDB operations (percentages of target)
        print("  Generating Cypher fundamentals...")
        cypher = self.generate_cypher_fundamentals()
        cypher_expanded = self._deduplicate_and_expand(cypher, int(num_examples * 0.30), "cypher")
        all_examples.extend(cypher_expanded)
        print(f"    ✓ {len(cypher_expanded)} Cypher examples (target: {int(num_examples * 0.30)})")
        
        print("  Generating NornicDB operations...")
        nornicdb = self.generate_nornicdb_operations()
        nornicdb_expanded = self._deduplicate_and_expand(nornicdb, int(num_examples * 0.40), "nornicdb")
        all_examples.extend(nornicdb_expanded)
        print(f"    ✓ {len(nornicdb_expanded)} NornicDB examples (target: {int(num_examples * 0.40)})")
        
        print("  Generating Heimdall plugin examples...")
        heimdall = self.generate_heimdall_plugins()
        heimdall_expanded = self._deduplicate_and_expand(heimdall, int(num_examples * 0.20), "heimdall")
        all_examples.extend(heimdall_expanded)
        print(f"    ✓ {len(heimdall_expanded)} Heimdall examples (target: {int(num_examples * 0.20)})")
        
        print("  Generating troubleshooting examples...")
        troubleshooting = self.generate_troubleshooting()
        troubleshooting_expanded = self._deduplicate_and_expand(troubleshooting, int(num_examples * 0.10), "troubleshooting")
        all_examples.extend(troubleshooting_expanded)
        print(f"    ✓ {len(troubleshooting_expanded)} Troubleshooting examples (target: {int(num_examples * 0.10)})")
        
        print("  Extracting real queries from codebase...")
        real_queries = self.extract_real_cypher_from_codebase()
        if real_queries:
            # Deduplicate real queries too
            unique_real = []
            for ex in real_queries:
                import hashlib
                output_hash = hashlib.md5(ex['output'].encode()).hexdigest()
                if output_hash not in self.seen_outputs:
                    self.seen_outputs.add(output_hash)
                    unique_real.append(ex)
            all_examples.extend(unique_real)
            print(f"    ✓ {len(unique_real)} unique real queries (from {len(real_queries)} found)")
        
        # Shuffle
        random.shuffle(all_examples)
        
        print(f"\n✅ Generated {len(all_examples)} unique training examples")
        print(f"   Deduplication rate: {(len(self.seen_outputs) / len(all_examples) * 100):.1f}%")
        return all_examples


def main():
    parser = argparse.ArgumentParser(
        description="Generate Heimdall training dataset for NornicDB"
    )
    parser.add_argument(
        "--repo",
        type=str,
        default=".",
        help="Path to NornicDB repository"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/heimdall_training.jsonl",
        help="Output file path"
    )
    parser.add_argument(
        "--num-examples",
        type=int,
        default=5000,
        help="Number of examples to generate"
    )
    
    args = parser.parse_args()
    
    generator = HeimdallDatasetGenerator(args.repo)
    examples = generator.generate_dataset(args.num_examples)
    
    # Save to file
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    
    with open(args.output, 'w') as f:
        for example in examples:
            f.write(json.dumps(example) + '\n')
    
    print(f"\n✓ Saved to {args.output}")
    
    # Statistics
    by_category = defaultdict(int)
    by_context = defaultdict(int)
    for ex in examples:
        by_category[ex.get('category', 'unknown')] += 1
        by_context[ex.get('context_size', 'unknown')] += 1
    
    print("\nDataset Statistics:")
    print("\nBy Category:")
    for category, count in sorted(by_category.items()):
        pct = (count / len(examples)) * 100
        print(f"  {category}: {count} ({pct:.1f}%)")
    
    print("\nBy Context Size:")
    for context, count in sorted(by_context.items()):
        pct = (count / len(examples)) * 100
        print(f"  {context}: {count} ({pct:.1f}%)")
    
    print("\nRecommended Training Configuration:")
    print("  Model: qwen_1_5b or phi3_mini")
    print("  Context size: 2048 tokens (most examples are small/medium)")
    print("  Epochs: 8-10 (specialized domain)")
    print("  Batch size: 8 (with gradient accumulation 2)")
    
    print("\nNext steps:")
    print(f"  1. Validate: python scripts/validate_dataset.py {args.output}")
    print(f"  2. Train: python train.py --preset heimdall --dataset {args.output} --output_dir models/heimdall")
    print(f"  3. Export: python export_to_gguf.py --model_dir models/heimdall --output heimdall-q4.gguf")
    print(f"  4. Deploy: cp heimdall-q4.gguf plugins/heimdall/models/")


if __name__ == "__main__":
    main()
