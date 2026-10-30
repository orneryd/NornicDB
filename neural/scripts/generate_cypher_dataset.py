#!/usr/bin/env python3
"""
Generate synthetic Cypher query training data.
Creates diverse examples covering all major Cypher patterns.
"""

import json
import random
import argparse
from typing import List, Dict, Tuple


class CypherDatasetGenerator:
    """
    Generate Cypher query training examples by enumerating all valid syntax constructs.
    Programmatically covers all Cypher language features from Neo4j specification.
    """
    
    def __init__(self, seed: int = 42):
        random.seed(seed)
        
        # Common graph labels
        self.labels = [
            "Person", "User", "Employee", "Customer", "Student",
            "Product", "Order", "Company", "Project", "Task",
            "Post", "Comment", "Message", "File", "Document"
        ]
        
        # Common properties by label
        self.properties = {
            "Person": ["name", "age", "email", "city", "country", "phone"],
            "User": ["username", "email", "status", "created", "role", "last_login"],
            "Employee": ["name", "department", "salary", "hire_date", "employee_id"],
            "Product": ["name", "price", "category", "stock", "rating", "sku"],
            "Order": ["order_id", "total", "status", "created", "customer_id"],
            "Customer": ["name", "email", "phone", "address", "join_date"],
            "Project": ["name", "status", "budget", "start_date", "end_date"],
        }
        
        # Property types for generation
        self.property_types = {
            "name": "string",
            "age": "integer",
            "email": "string",
            "price": "float",
            "status": "string",
            "created": "datetime",
            "salary": "integer",
        }
        
        # Common relationships with labels they connect
        self.relationships = {
            "Person": ["KNOWS", "FRIEND_OF", "WORKS_WITH", "MANAGES", "LIVES_IN"],
            "User": ["FOLLOWS", "LIKES", "POSTED", "COMMENTED", "SHARED"],
            "Employee": ["WORKS_FOR", "MANAGES", "REPORTS_TO", "COLLABORATES_WITH"],
            "Product": ["BELONGS_TO", "RELATED_TO", "PURCHASED_WITH"],
            "Customer": ["PURCHASED", "REVIEWED", "VIEWED", "FAVORITED"],
            "Project": ["HAS_MEMBER", "DEPENDS_ON", "FUNDED_BY"],
        }
        
        # Sample values
        self.names = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Helen", "Ivan"]
        self.cities = ["New York", "London", "Tokyo", "Paris", "Berlin", "Sydney", "Toronto", "Mumbai"]
        self.statuses = ["active", "inactive", "pending", "completed", "cancelled", "draft", "published"]
        self.operators = ["=", "<", ">", "<=", ">=", "<>", "STARTS WITH", "ENDS WITH", "CONTAINS"]
        
        # Cypher keywords and patterns we need to cover
        self.cypher_clauses = [
            "MATCH", "OPTIONAL MATCH", "CREATE", "MERGE", "DELETE", "DETACH DELETE",
            "SET", "REMOVE", "WITH", "UNWIND", "RETURN", "ORDER BY", "LIMIT", "SKIP",
            "WHERE", "DISTINCT", "UNION", "UNION ALL"
        ]
        
        self.cypher_functions = {
            "aggregation": ["count", "sum", "avg", "min", "max", "collect"],
            "scalar": ["size", "length", "type", "id", "labels", "keys", "properties"],
            "list": ["head", "tail", "range", "reverse"],
            "string": ["toUpper", "toLower", "trim", "substring", "replace"],
            "mathematical": ["abs", "ceil", "floor", "round", "sqrt", "rand"],
            "predicate": ["exists", "isEmpty", "all", "any", "none", "single"],
        }
        
        self.cypher_patterns = {
            "node": ["(n)", "(n:Label)", "(n:Label {prop: value})", "(:Label)", "(n:Label1:Label2)"],
            "relationship": ["-[r]->", "-[r:TYPE]->", "-[r:TYPE {prop: value}]->", "-[:TYPE]->", "<-[r]-", "-[r]-"],
            "path": ["-[*]->", "-[*1..3]->", "-[*..5]->", "shortestPath()"],
        }
        
    def generate_basic_match(self, count: int) -> List[Dict]:
        """Generate basic MATCH queries."""
        examples = []
        
        templates = [
            ("Find all {label} nodes", "MATCH (n:{label}) RETURN n"),
            ("Get all {label}", "MATCH (n:{label}) RETURN n"),
            ("Show me all {label}", "MATCH (n:{label}) RETURN n"),
            ("List all {label}", "MATCH (n:{label}) RETURN n"),
            ("Retrieve all {label}", "MATCH (n:{label}) RETURN n"),
        ]
        
        for _ in range(count):
            template = random.choice(templates)
            label = random.choice(self.labels)
            examples.append({
                "instruction": "Convert this natural language query to Cypher",
                "input": template[0].format(label=label),
                "output": template[1].format(label=label)
            })
        
        return examples
    
    def generate_property_match(self, count: int) -> List[Dict]:
        """Generate MATCH queries with property filters."""
        examples = []
        
        templates = [
            ("Find {label} where {prop} is {value}", 
             "MATCH (n:{label} {{{prop}: '{value}'}}) RETURN n"),
            ("Get {label} with {prop} equal to {value}",
             "MATCH (n:{label}) WHERE n.{prop} = '{value}' RETURN n"),
            ("Show {label} that have {prop} of {value}",
             "MATCH (n:{label} {{{prop}: '{value}'}}) RETURN n"),
        ]
        
        for _ in range(count):
            template = random.choice(templates)
            label = random.choice(self.labels)
            props = self.properties.get(label, ["name", "status", "type"])
            prop = random.choice(props)
            value = random.choice(self.names + self.cities + self.statuses)
            
            examples.append({
                "instruction": "Convert this natural language query to Cypher",
                "input": template[0].format(label=label, prop=prop, value=value),
                "output": template[1].format(label=label, prop=prop, value=value)
            })
        
        return examples
    
    def generate_relationship_queries(self, count: int) -> List[Dict]:
        """Generate relationship traversal queries."""
        examples = []
        
        # Direct relationships
        templates = [
            ("Find what {name} {rel_lower}",
             "MATCH (n:{label} {{name: '{name}'}})-[:{rel}]->(m) RETURN m"),
            ("Show all {label2} that {name} {rel_lower}",
             "MATCH (n:{label} {{name: '{name}'}})-[:{rel}]->(m:{label2}) RETURN m"),
            ("Get {rel_lower} relationships for {name}",
             "MATCH (n:{label} {{name: '{name}'}})-[r:{rel}]->(m) RETURN n, r, m"),
        ]
        
        for _ in range(count // 2):
            template = random.choice(templates)
            label = random.choice(["Person", "User", "Employee"])
            rels = self.relationships.get(label, ["RELATES_TO"])
            rel = random.choice(rels)
            rel_lower = rel.lower().replace("_", " ")
            name = random.choice(self.names)
            label2 = random.choice(self.labels)
            
            examples.append({
                "instruction": "Convert this natural language query to Cypher",
                "input": template[0].format(
                    name=name, rel_lower=rel_lower, label=label, rel=rel, label2=label2
                ),
                "output": template[1].format(
                    name=name, label=label, rel=rel, label2=label2
                )
            })
        
        # Multi-hop relationships
        multi_hop = [
            ("Find friends of friends of {name}",
             "MATCH (n:Person {{name: '{name}'}})-[:KNOWS]->()-[:KNOWS]->(fof) RETURN DISTINCT fof"),
            ("Get second-degree connections for {name}",
             "MATCH (n:Person {{name: '{name}'}})-[:KNOWS*2]->(connection) RETURN DISTINCT connection"),
        ]
        
        for _ in range(count // 2):
            template = random.choice(multi_hop)
            name = random.choice(self.names)
            examples.append({
                "instruction": "Convert this natural language query to Cypher",
                "input": template[0].format(name=name),
                "output": template[1].format(name=name)
            })
        
        return examples
    
    def generate_aggregation(self, count: int) -> List[Dict]:
        """Generate aggregation queries."""
        examples = []
        
        templates = [
            ("Count all {label}", 
             "MATCH (n:{label}) RETURN count(n)"),
            ("How many {label} are there",
             "MATCH (n:{label}) RETURN count(n) as total"),
            ("Count {label} by {prop}",
             "MATCH (n:{label}) RETURN n.{prop}, count(n) as count"),
            ("Get average {prop} of {label}",
             "MATCH (n:{label}) RETURN avg(n.{prop}) as average"),
            ("Find total {prop} for all {label}",
             "MATCH (n:{label}) RETURN sum(n.{prop}) as total"),
            ("Get min and max {prop} of {label}",
             "MATCH (n:{label}) RETURN min(n.{prop}) as min, max(n.{prop}) as max"),
        ]
        
        for _ in range(count):
            template = random.choice(templates)
            label = random.choice(self.labels)
            props = self.properties.get(label, ["age", "price", "count"])
            prop = random.choice(props)
            
            examples.append({
                "instruction": "Convert this natural language query to Cypher",
                "input": template[0].format(label=label, prop=prop),
                "output": template[1].format(label=label, prop=prop)
            })
        
        return examples
    
    def generate_filtering(self, count: int) -> List[Dict]:
        """Generate queries with WHERE clauses."""
        examples = []
        
        templates = [
            ("Find {label} older than {value}",
             "MATCH (n:{label}) WHERE n.age > {value} RETURN n"),
            ("Get {label} with {prop} less than {value}",
             "MATCH (n:{label}) WHERE n.{prop} < {value} RETURN n"),
            ("Show {label} where {prop} starts with {letter}",
             "MATCH (n:{label}) WHERE n.{prop} STARTS WITH '{letter}' RETURN n"),
            ("Find {label} with {prop} containing {substr}",
             "MATCH (n:{label}) WHERE n.{prop} CONTAINS '{substr}' RETURN n"),
            ("Get {label} where {prop} is null",
             "MATCH (n:{label}) WHERE n.{prop} IS NULL RETURN n"),
            ("Find {label} where {prop} is not null",
             "MATCH (n:{label}) WHERE n.{prop} IS NOT NULL RETURN n"),
        ]
        
        for _ in range(count):
            template = random.choice(templates)
            label = random.choice(self.labels)
            props = self.properties.get(label, ["name", "status", "age"])
            prop = random.choice(props)
            value = random.randint(20, 50)
            letter = random.choice("ABCDEFGH")
            substr = random.choice(["test", "prod", "admin", "user"])
            
            examples.append({
                "instruction": "Convert this natural language query to Cypher",
                "input": template[0].format(
                    label=label, prop=prop, value=value, letter=letter, substr=substr
                ),
                "output": template[1].format(
                    label=label, prop=prop, value=value, letter=letter, substr=substr
                )
            })
        
        return examples
    
    def generate_create_queries(self, count: int) -> List[Dict]:
        """Generate CREATE queries."""
        examples = []
        
        templates = [
            ("Create a {label} named {name}",
             "CREATE (n:{label} {{name: '{name}'}})"),
            ("Add a new {label} with {prop} of {value}",
             "CREATE (n:{label} {{{prop}: '{value}'}})"),
            ("Create {label} with name {name} and {prop} {value}",
             "CREATE (n:{label} {{name: '{name}', {prop}: '{value}'}})"),
        ]
        
        for _ in range(count):
            template = random.choice(templates)
            label = random.choice(self.labels)
            name = random.choice(self.names)
            props = self.properties.get(label, ["status", "type"])
            prop = random.choice(props)
            value = random.choice(self.statuses + [str(random.randint(1, 100))])
            
            examples.append({
                "instruction": "Convert this natural language query to Cypher",
                "input": template[0].format(label=label, name=name, prop=prop, value=value),
                "output": template[1].format(label=label, name=name, prop=prop, value=value)
            })
        
        return examples
    
    def generate_complex_queries(self, count: int) -> List[Dict]:
        """Generate complex multi-clause queries."""
        examples = []
        
        complex_templates = [
            ("Find the person with the most friends",
             "MATCH (p:Person)-[:KNOWS]->() RETURN p, count(*) as friends ORDER BY friends DESC LIMIT 1"),
            ("Get top 5 most active users",
             "MATCH (u:User)-[:POSTED]->(p) RETURN u, count(p) as posts ORDER BY posts DESC LIMIT 5"),
            ("Find products purchased together with product {name}",
             "MATCH (p:Product {{name: '{name}'}})<-[:PURCHASED]-(c:Customer)-[:PURCHASED]->(other:Product) WHERE p <> other RETURN other, count(*) as frequency ORDER BY frequency DESC"),
            ("Shortest path from {name1} to {name2}",
             "MATCH path = shortestPath((a:Person {{name: '{name1}'}})-[*]-(b:Person {{name: '{name2}'}})) RETURN path"),
        ]
        
        for _ in range(count):
            template = random.choice(complex_templates)
            name = random.choice(self.names)
            name1 = random.choice(self.names)
            name2 = random.choice([n for n in self.names if n != name1])
            
            examples.append({
                "instruction": "Convert this natural language query to Cypher",
                "input": template[0].format(name=name, name1=name1, name2=name2),
                "output": template[1].format(name=name, name1=name1, name2=name2)
            })
        
        return examples
    
    def enumerate_all_syntax(self, count: int) -> List[Dict]:
        """
        Systematically enumerate ALL valid Cypher syntax constructs.
        This ensures comprehensive coverage of the language.
        """
        examples = []
        
        # 1. All MATCH patterns
        for label in self.labels[:5]:  # Sample labels
            props = self.properties.get(label, ["name", "status"])
            prop = random.choice(props)
            value = random.choice(self.names + self.statuses)
            
            # Simple node match
            examples.append({
                "instruction": "Convert to Cypher",
                "input": f"Find all {label} nodes",
                "output": f"MATCH (n:{label}) RETURN n"
            })
            
            # Node with property
            examples.append({
                "instruction": "Convert to Cypher",
                "input": f"Find {label} where {prop} equals {value}",
                "output": f"MATCH (n:{label} {{{prop}: '{value}'}}) RETURN n"
            })
            
            # Node with WHERE clause
            for op in ["<", ">", "<=", ">="]:
                if prop in ["age", "price", "salary"]:
                    num = random.randint(20, 100)
                    examples.append({
                        "instruction": "Convert to Cypher",
                        "input": f"Find {label} where {prop} {op} {num}",
                        "output": f"MATCH (n:{label}) WHERE n.{prop} {op} {num} RETURN n"
                    })
        
        # 2. All relationship patterns
        for label in self.labels[:3]:
            rels = self.relationships.get(label, ["RELATES_TO"])
            for rel in rels:
                examples.append({
                    "instruction": "Convert to Cypher",
                    "input": f"Find all {label} with {rel.lower().replace('_', ' ')} relationships",
                    "output": f"MATCH (n:{label})-[r:{rel}]->(m) RETURN n, r, m"
                })
                
                # Variable length paths
                examples.append({
                    "instruction": "Convert to Cypher",
                    "input": f"Find {label} connected by 1 to 3 {rel.lower().replace('_', ' ')} hops",
                    "output": f"MATCH (n:{label})-[:{rel}*1..3]->(m) RETURN n, m"
                })
        
        # 3. All aggregation functions
        for func in self.cypher_functions["aggregation"]:
            label = random.choice(self.labels)
            if func in ["sum", "avg"]:
                prop = "price" if label == "Product" else "age"
                examples.append({
                    "instruction": "Convert to Cypher",
                    "input": f"Calculate {func} of {prop} for all {label}",
                    "output": f"MATCH (n:{label}) RETURN {func}(n.{prop}) as result"
                })
            elif func == "count":
                examples.append({
                    "instruction": "Convert to Cypher",
                    "input": f"Count all {label} nodes",
                    "output": f"MATCH (n:{label}) RETURN {func}(n) as total"
                })
            elif func == "collect":
                prop = random.choice(["name", "email"])
                examples.append({
                    "instruction": "Convert to Cypher",
                    "input": f"Collect all {prop} values from {label}",
                    "output": f"MATCH (n:{label}) RETURN {func}(n.{prop}) as values"
                })
        
        # 4. All string functions
        for func in self.cypher_functions["string"]:
            label = random.choice(self.labels)
            prop = "name"
            if func in ["toUpper", "toLower"]:
                examples.append({
                    "instruction": "Convert to Cypher",
                    "input": f"Get {label} names in {func[2:].lower()}",
                    "output": f"MATCH (n:{label}) RETURN {func}(n.{prop}) as name"
                })
        
        # 5. CREATE variations
        for label in self.labels[:3]:
            props = self.properties.get(label, ["name", "status"])
            
            # Simple create
            examples.append({
                "instruction": "Convert to Cypher",
                "input": f"Create a new {label} node",
                "output": f"CREATE (n:{label}) RETURN n"
            })
            
            # Create with properties
            name = random.choice(self.names)
            status = random.choice(self.statuses)
            examples.append({
                "instruction": "Convert to Cypher",
                "input": f"Create {label} named {name} with status {status}",
                "output": f"CREATE (n:{label} {{name: '{name}', status: '{status}'}}) RETURN n"
            })
        
        # 6. MERGE variations
        for label in self.labels[:2]:
            name = random.choice(self.names)
            examples.append({
                "instruction": "Convert to Cypher",
                "input": f"Find or create {label} named {name}",
                "output": f"MERGE (n:{label} {{name: '{name}'}}) RETURN n"
            })
            
            # MERGE with ON CREATE/ON MATCH
            examples.append({
                "instruction": "Convert to Cypher",
                "input": f"Find or create {label} {name}, set created timestamp on create",
                "output": f"MERGE (n:{label} {{name: '{name}'}}) ON CREATE SET n.created = timestamp() RETURN n"
            })
        
        # 7. DELETE variations
        label = random.choice(self.labels)
        name = random.choice(self.names)
        examples.append({
            "instruction": "Convert to Cypher",
            "input": f"Delete {label} named {name}",
            "output": f"MATCH (n:{label} {{name: '{name}'}}) DELETE n"
        })
        
        examples.append({
            "instruction": "Convert to Cypher",
            "input": f"Delete {label} and all its relationships",
            "output": f"MATCH (n:{label}) DETACH DELETE n"
        })
        
        # 8. SET/REMOVE variations
        examples.append({
            "instruction": "Convert to Cypher",
            "input": f"Update {label} {name} set status to active",
            "output": f"MATCH (n:{label} {{name: '{name}'}}) SET n.status = 'active' RETURN n"
        })
        
        examples.append({
            "instruction": "Convert to Cypher",
            "input": f"Add label NewLabel to {label} nodes",
            "output": f"MATCH (n:{label}) SET n:NewLabel RETURN n"
        })
        
        # 9. ORDER BY, LIMIT, SKIP
        for label in self.labels[:2]:
            prop = random.choice(self.properties.get(label, ["name", "created"]))
            examples.append({
                "instruction": "Convert to Cypher",
                "input": f"Get top 10 {label} ordered by {prop}",
                "output": f"MATCH (n:{label}) RETURN n ORDER BY n.{prop} LIMIT 10"
            })
            
            examples.append({
                "instruction": "Convert to Cypher",
                "input": f"Get {label} from 20 to 30 ordered by {prop}",
                "output": f"MATCH (n:{label}) RETURN n ORDER BY n.{prop} SKIP 20 LIMIT 10"
            })
        
        # 10. WITH clause (chaining)
        label = random.choice(self.labels)
        examples.append({
            "instruction": "Convert to Cypher",
            "input": f"Find {label} with more than 5 relationships",
            "output": f"MATCH (n:{label})-[r]->() WITH n, count(r) as rels WHERE rels > 5 RETURN n, rels"
        })
        
        # 11. OPTIONAL MATCH
        examples.append({
            "instruction": "Convert to Cypher",
            "input": f"Find all {label} and optionally their relationships",
            "output": f"MATCH (n:{label}) OPTIONAL MATCH (n)-[r]->() RETURN n, r"
        })
        
        # 12. UNION
        label1, label2 = random.sample(self.labels, 2)
        examples.append({
            "instruction": "Convert to Cypher",
            "input": f"Get names from both {label1} and {label2}",
            "output": f"MATCH (n:{label1}) RETURN n.name as name UNION MATCH (m:{label2}) RETURN m.name as name"
        })
        
        # Shuffle and sample
        random.shuffle(examples)
        return examples[:count]
    
    def generate_dataset(self, total_count: int = 1000) -> List[Dict]:
        """
        Generate complete dataset with balanced distribution.
        Now includes comprehensive syntax enumeration.
        """
        distribution = {
            "basic_match": 0.15,      # 15% basic queries
            "property_match": 0.20,   # 20% property filtering
            "relationships": 0.15,    # 15% relationship traversal
            "aggregation": 0.12,      # 12% aggregations
            "filtering": 0.10,        # 10% WHERE clauses
            "create": 0.05,           # 5% CREATE queries
            "complex": 0.05,          # 5% complex queries
            "syntax_enum": 0.18,      # 18% systematic syntax coverage
        }
        
        examples = []
        examples.extend(self.generate_basic_match(int(total_count * distribution["basic_match"])))
        examples.extend(self.generate_property_match(int(total_count * distribution["property_match"])))
        examples.extend(self.generate_relationship_queries(int(total_count * distribution["relationships"])))
        examples.extend(self.generate_aggregation(int(total_count * distribution["aggregation"])))
        examples.extend(self.generate_filtering(int(total_count * distribution["filtering"])))
        examples.extend(self.generate_create_queries(int(total_count * distribution["create"])))
        examples.extend(self.generate_complex_queries(int(total_count * distribution["complex"])))
        examples.extend(self.enumerate_all_syntax(int(total_count * distribution["syntax_enum"])))
        
        # Shuffle to mix query types
        random.shuffle(examples)
        
        return examples


def main():
    parser = argparse.ArgumentParser(description="Generate Cypher training dataset")
    parser.add_argument("--output", type=str, default="data/cypher_training.jsonl",
                      help="Output file path")
    parser.add_argument("--count", type=int, default=1000,
                      help="Number of examples to generate")
    parser.add_argument("--seed", type=int, default=42,
                      help="Random seed for reproducibility")
    
    args = parser.parse_args()
    
    # Generate dataset
    print(f"Generating {args.count} Cypher training examples...")
    generator = CypherDatasetGenerator(seed=args.seed)
    examples = generator.generate_dataset(args.count)
    
    # Save to file
    import os
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    
    with open(args.output, 'w') as f:
        for example in examples:
            f.write(json.dumps(example) + '\n')
    
    print(f"✓ Generated {len(examples)} examples")
    print(f"✓ Saved to: {args.output}")
    
    # Print statistics
    print("\nDataset Statistics:")
    print(f"  Total examples: {len(examples)}")
    print(f"  Avg input length: {sum(len(e['input']) for e in examples) / len(examples):.1f} chars")
    print(f"  Avg output length: {sum(len(e['output']) for e in examples) / len(examples):.1f} chars")
    
    # Sample examples
    print("\nSample Examples:")
    for i, example in enumerate(random.sample(examples, 3), 1):
        print(f"\n  Example {i}:")
        print(f"    Input:  {example['input']}")
        print(f"    Output: {example['output']}")


if __name__ == "__main__":
    main()
