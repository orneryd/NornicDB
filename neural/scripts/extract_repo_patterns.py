#!/usr/bin/env python3
"""
Extract training data from a code repository.
Analyzes code patterns, function implementations, API usage, and documentation.

Supports: Go, Python, JavaScript, Java
"""

import os
import json
import argparse
import re
from pathlib import Path
from typing import List, Dict, Set, Tuple
from collections import defaultdict
import hashlib


class RepositoryAnalyzer:
    """Analyze repository and extract training patterns."""
    
    def __init__(self, repo_path: str, language: str = "go"):
        self.repo_path = Path(repo_path)
        self.language = language
        self.examples = []
        self.file_extensions = {
            "go": [".go"],
            "python": [".py"],
            "javascript": [".js", ".ts", ".jsx", ".tsx"],
            "java": [".java"]
        }
        
    def extract_all(self) -> List[Dict]:
        """Run all extraction methods."""
        print(f"Analyzing repository: {self.repo_path}")
        print(f"Language: {self.language}")
        
        self.extract_function_patterns()
        self.extract_type_definitions()
        self.extract_test_patterns()
        self.extract_documentation()
        self.extract_api_usage()
        self.extract_error_patterns()
        self.generate_qa_pairs()
        
        print(f"✓ Extracted {len(self.examples)} training examples")
        return self.examples
    
    def extract_function_patterns(self):
        """Extract function signatures and implementations."""
        print("Extracting function patterns...")
        
        extensions = self.file_extensions[self.language]
        count = 0
        
        for ext in extensions:
            for file_path in self.repo_path.rglob(f"*{ext}"):
                # Skip test files and vendor directories
                if "_test" in str(file_path) or "vendor" in str(file_path):
                    continue
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    if self.language == "go":
                        functions = self.extract_go_functions(content, file_path)
                    elif self.language == "python":
                        functions = self.extract_python_functions(content, file_path)
                    
                    self.examples.extend(functions)
                    count += len(functions)
                    
                except Exception as e:
                    pass  # Skip files that can't be read
        
        print(f"  Found {count} functions")
    
    def extract_go_functions(self, content: str, file_path: Path) -> List[Dict]:
        """Extract Go function patterns."""
        examples = []
        
        # Regex for Go functions with comments
        pattern = r'(?://\s*(.+?)\n)?func\s+(?:\([^)]+\)\s+)?(\w+)\s*\(([^)]*)\)\s*(?:\(([^)]*)\)|([^\s{]+))?\s*{'
        
        matches = re.finditer(pattern, content)
        
        for match in matches:
            comment = match.group(1) or ""
            func_name = match.group(2)
            params = match.group(3)
            returns = match.group(4) or match.group(5) or ""
            
            # Skip very short or test functions
            if len(func_name) < 3 or func_name.startswith("Test"):
                continue
            
            # Create example from comment (if good quality)
            if comment and len(comment) > 20:
                signature = f"func {func_name}({params})"
                if returns:
                    signature += f" {returns}"
                
                examples.append({
                    "instruction": "Implement this Go function",
                    "input": f"{signature} {{\n    // {comment}\n}}",
                    "output": f"// Implementation based on: {file_path.relative_to(self.repo_path)}\n{signature}",
                    "category": "go_function",
                    "file": str(file_path.relative_to(self.repo_path))
                })
        
        return examples
    
    def extract_python_functions(self, content: str, file_path: Path) -> List[Dict]:
        """Extract Python function patterns."""
        examples = []
        
        # Regex for Python functions with docstrings
        pattern = r'def\s+(\w+)\s*\(([^)]*)\)\s*(?:->\s*([^:]+))?\s*:\s*"""([^"]+)"""'
        
        matches = re.finditer(pattern, content, re.MULTILINE | re.DOTALL)
        
        for match in matches:
            func_name = match.group(1)
            params = match.group(2)
            returns = match.group(3) or ""
            docstring = match.group(4).strip()
            
            if len(docstring) > 20:
                signature = f"def {func_name}({params})"
                if returns:
                    signature += f" -> {returns}"
                
                examples.append({
                    "instruction": "Implement this Python function",
                    "input": f'{signature}:\n    """{docstring}"""\n    pass',
                    "output": f"# Implementation based on: {file_path.relative_to(self.repo_path)}",
                    "category": "python_function",
                    "file": str(file_path.relative_to(self.repo_path))
                })
        
        return examples
    
    def extract_type_definitions(self):
        """Extract type/struct/class definitions."""
        print("Extracting type definitions...")
        
        count = 0
        extensions = self.file_extensions[self.language]
        
        for ext in extensions:
            for file_path in self.repo_path.rglob(f"*{ext}"):
                if "vendor" in str(file_path):
                    continue
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    if self.language == "go":
                        # Extract Go structs
                        struct_pattern = r'type\s+(\w+)\s+struct\s*{([^}]+)}'
                        for match in re.finditer(struct_pattern, content):
                            struct_name = match.group(1)
                            fields = match.group(2)
                            
                            self.examples.append({
                                "instruction": "Explain this Go struct",
                                "input": f"type {struct_name} struct {{{fields}}}",
                                "output": f"This is the {struct_name} struct from {file_path.relative_to(self.repo_path)}",
                                "category": "type_definition",
                                "file": str(file_path.relative_to(self.repo_path))
                            })
                            count += 1
                    
                except Exception as e:
                    pass
        
        print(f"  Found {count} type definitions")
    
    def extract_test_patterns(self):
        """Extract test patterns and link to implementations."""
        print("Extracting test patterns...")
        
        count = 0
        extensions = self.file_extensions[self.language]
        
        for ext in extensions:
            for file_path in self.repo_path.rglob(f"*_test{ext}"):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    # Find test functions
                    if self.language == "go":
                        test_pattern = r'func\s+Test(\w+)\s*\([^)]+\)\s*{'
                        for match in re.finditer(test_pattern, content):
                            test_name = match.group(1)
                            
                            self.examples.append({
                                "instruction": "Explain this test",
                                "input": f"Test{test_name}",
                                "output": f"This test validates the {test_name} functionality in {file_path.parent}",
                                "category": "test",
                                "file": str(file_path.relative_to(self.repo_path))
                            })
                            count += 1
                    
                except Exception as e:
                    pass
        
        print(f"  Found {count} test patterns")
    
    def extract_documentation(self):
        """Extract documentation and README content."""
        print("Extracting documentation...")
        
        doc_files = list(self.repo_path.rglob("*.md"))
        count = 0
        
        for doc_file in doc_files:
            try:
                with open(doc_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Extract headings and their content
                sections = re.findall(r'#+\s+(.+?)\n\n(.+?)(?=\n#+|\Z)', content, re.DOTALL)
                
                for heading, text in sections:
                    if len(text) > 100:  # Meaningful content
                        self.examples.append({
                            "instruction": "Answer this documentation question",
                            "input": f"What does the {heading} section explain?",
                            "output": text[:500],  # Limit length
                            "category": "documentation",
                            "file": str(doc_file.relative_to(self.repo_path))
                        })
                        count += 1
                
            except Exception as e:
                pass
        
        print(f"  Found {count} documentation sections")
    
    def extract_api_usage(self):
        """Extract common API usage patterns."""
        print("Extracting API usage patterns...")
        
        # Find common patterns like initialization, error handling, etc.
        patterns = defaultdict(list)
        count = 0
        
        extensions = self.file_extensions[self.language]
        
        for ext in extensions:
            for file_path in self.repo_path.rglob(f"*{ext}"):
                if "vendor" in str(file_path) or "_test" in str(file_path):
                    continue
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    # Go: Find error handling patterns
                    if self.language == "go":
                        error_pattern = r'if\s+err\s*:?=.*?{\s*return.*?}'
                        matches = re.findall(error_pattern, content, re.DOTALL)
                        for match in matches[:3]:  # Limit per file
                            patterns['error_handling'].append(match)
                            count += 1
                    
                except Exception as e:
                    pass
        
        # Convert patterns to examples
        for pattern_type, instances in patterns.items():
            if instances:
                # Sample unique instances
                unique = list(set(instances))[:10]
                for instance in unique:
                    self.examples.append({
                        "instruction": f"Show example of {pattern_type} in {self.language}",
                        "input": f"How do you handle {pattern_type}?",
                        "output": instance,
                        "category": "api_usage",
                    })
        
        print(f"  Found {count} API usage patterns")
    
    def extract_error_patterns(self):
        """Extract error handling and validation patterns."""
        print("Extracting error patterns...")
        
        # This would extract common error handling approaches
        # from the codebase to teach the model proper error handling
        pass
    
    def generate_qa_pairs(self):
        """Generate Q&A pairs about the repository."""
        print("Generating Q&A pairs...")
        
        # Read README for project overview
        readme_path = self.repo_path / "README.md"
        if readme_path.exists():
            with open(readme_path, 'r', encoding='utf-8') as f:
                readme = f.read()
            
            # Generate basic Q&A
            repo_name = self.repo_path.name
            
            self.examples.extend([
                {
                    "instruction": f"Answer this question about {repo_name}",
                    "input": f"What is {repo_name}?",
                    "output": f"{repo_name} is a project in the {self.repo_path} repository.",
                    "category": "qa"
                },
                {
                    "instruction": f"Answer this question about {repo_name}",
                    "input": f"What language is {repo_name} written in?",
                    "output": f"{repo_name} is primarily written in {self.language}.",
                    "category": "qa"
                }
            ])


def main():
    parser = argparse.ArgumentParser(
        description="Extract training data from code repository"
    )
    parser.add_argument(
        "--repo",
        type=str,
        required=True,
        help="Path to repository"
    )
    parser.add_argument(
        "--language",
        type=str,
        default="go",
        choices=["go", "python", "javascript", "java"],
        help="Primary language of repository"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/repo_patterns.jsonl",
        help="Output file path"
    )
    parser.add_argument(
        "--include-docs",
        action="store_true",
        help="Include documentation in training data"
    )
    
    args = parser.parse_args()
    
    # Analyze repository
    analyzer = RepositoryAnalyzer(args.repo, args.language)
    examples = analyzer.extract_all()
    
    # Save to file
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    
    with open(args.output, 'w') as f:
        for example in examples:
            f.write(json.dumps(example) + '\n')
    
    print(f"\n✓ Saved {len(examples)} examples to {args.output}")
    
    # Statistics
    by_category = defaultdict(int)
    for ex in examples:
        by_category[ex.get('category', 'unknown')] += 1
    
    print("\nDataset Statistics:")
    for category, count in sorted(by_category.items()):
        print(f"  {category}: {count}")
    
    print("\nNext steps:")
    print(f"  1. Validate: python scripts/validate_dataset.py {args.output}")
    print(f"  2. Train: python train.py --dataset {args.output} --output_dir models/repo-expert")
    print(f"  3. Export: python export_to_gguf.py --model_dir models/repo-expert --output repo-expert-q4.gguf")


if __name__ == "__main__":
    main()
