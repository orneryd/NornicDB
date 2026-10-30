#!/usr/bin/env python3
"""
Generate coding assistant training data.
Covers: code generation, bug fixing, code understanding, test generation.

Supports: Python, Go, JavaScript, Java
"""

import json
import random
import argparse
from typing import List, Dict
from collections import defaultdict


class CodingDatasetGenerator:
    """Generate coding assistant training data."""
    
    def __init__(self, language: str = "python"):
        self.language = language
        self.examples = []
        self.seen_outputs = set()  # Track unique outputs
        
    def generate_code_generation(self) -> List[Dict]:
        """Generate code generation examples."""
        examples = []
        
        if self.language == "python":
            examples.extend([
                {
                    "instruction": "Implement this function",
                    "input": "Write a function to check if a string is a palindrome",
                    "output": '''def is_palindrome(s: str) -> bool:
    """Check if string is a palindrome."""
    # Remove spaces and convert to lowercase
    s = ''.join(s.split()).lower()
    # Compare string with its reverse
    return s == s[::-1]''',
                    "category": "code_generation"
                },
                {
                    "instruction": "Implement this function",
                    "input": "Write a function to find the nth Fibonacci number",
                    "output": '''def fibonacci(n: int) -> int:
    """Calculate nth Fibonacci number."""
    if n <= 1:
        return n
    
    # Use dynamic programming
    a, b = 0, 1
    for _ in range(2, n + 1):
        a, b = b, a + b
    return b''',
                    "category": "code_generation"
                },
                {
                    "instruction": "Implement this function",
                    "input": "Write a function to reverse a linked list",
                    "output": '''def reverse_linked_list(head: Node) -> Node:
    """Reverse a singly linked list."""
    prev = None
    current = head
    
    while current:
        next_node = current.next
        current.next = prev
        prev = current
        current = next_node
    
    return prev''',
                    "category": "code_generation"
                }
            ])
        
        elif self.language == "go":
            examples.extend([
                {
                    "instruction": "Implement this function",
                    "input": "Write a function to check if a string is a palindrome",
                    "output": '''func IsPalindrome(s string) bool {
    // Remove spaces and convert to lowercase
    s = strings.ToLower(strings.ReplaceAll(s, " ", ""))
    
    // Compare string with its reverse
    runes := []rune(s)
    for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
        if runes[i] != runes[j] {
            return false
        }
    }
    return true
}''',
                    "category": "code_generation"
                },
                {
                    "instruction": "Implement this function",
                    "input": "Write a function to find the nth Fibonacci number",
                    "output": '''func Fibonacci(n int) int {
    if n <= 1 {
        return n
    }
    
    // Use dynamic programming
    a, b := 0, 1
    for i := 2; i <= n; i++ {
        a, b = b, a+b
    }
    return b
}''',
                    "category": "code_generation"
                }
            ])
        
        return examples
    
    def generate_bug_fixing(self) -> List[Dict]:
        """Generate bug fixing examples."""
        examples = []
        
        if self.language == "python":
            examples.extend([
                {
                    "instruction": "Fix this bug",
                    "input": '''def calculate_average(numbers):
    total = 0
    for num in numbers:
        total += num
    return total / len(numbers)

# Bug: Crashes with empty list''',
                    "output": '''def calculate_average(numbers):
    """Calculate average of numbers list."""
    if not numbers:  # FIX: Handle empty list
        return 0
    
    total = 0
    for num in numbers:
        total += num
    return total / len(numbers)''',
                    "category": "bug_fixing"
                },
                {
                    "instruction": "Fix this bug",
                    "input": '''def find_max(numbers):
    max_val = 0
    for num in numbers:
        if num > max_val:
            max_val = num
    return max_val

# Bug: Fails with negative numbers''',
                    "output": '''def find_max(numbers):
    """Find maximum value in list."""
    if not numbers:
        raise ValueError("List cannot be empty")
    
    # FIX: Initialize with first element, not 0
    max_val = numbers[0]
    for num in numbers[1:]:
        if num > max_val:
            max_val = num
    return max_val''',
                    "category": "bug_fixing"
                },
                {
                    "instruction": "Fix this bug",
                    "input": '''def remove_duplicates(lst):
    for item in lst:
        if lst.count(item) > 1:
            lst.remove(item)
    return lst

# Bug: Doesn't remove all duplicates''',
                    "output": '''def remove_duplicates(lst):
    """Remove duplicates from list."""
    # FIX: Use set for O(n) instead of modifying during iteration
    return list(dict.fromkeys(lst))  # Preserves order

# Alternative with explicit set:
# return list(set(lst))  # Doesn't preserve order''',
                    "category": "bug_fixing"
                }
            ])
        
        elif self.language == "go":
            examples.extend([
                {
                    "instruction": "Fix this bug",
                    "input": '''func CalculateAverage(numbers []int) float64 {
    total := 0
    for _, num := range numbers {
        total += num
    }
    return float64(total) / float64(len(numbers))
}

// Bug: Crashes with empty slice''',
                    "output": '''func CalculateAverage(numbers []int) float64 {
    // FIX: Handle empty slice
    if len(numbers) == 0 {
        return 0
    }
    
    total := 0
    for _, num := range numbers {
        total += num
    }
    return float64(total) / float64(len(numbers))
}''',
                    "category": "bug_fixing"
                }
            ])
        
        return examples
    
    def generate_code_understanding(self) -> List[Dict]:
        """Generate code understanding examples."""
        examples = []
        
        if self.language == "python":
            examples.extend([
                {
                    "instruction": "Explain this code",
                    "input": '''def quicksort(arr):
    if len(arr) <= 1:
        return arr
    pivot = arr[len(arr) // 2]
    left = [x for x in arr if x < pivot]
    middle = [x for x in arr if x == pivot]
    right = [x for x in arr if x > pivot]
    return quicksort(left) + middle + quicksort(right)''',
                    "output": '''This implements the quicksort algorithm:
1. Base case: Arrays with 0-1 elements are already sorted
2. Choose pivot (middle element)
3. Partition into three groups:
   - left: elements less than pivot
   - middle: elements equal to pivot
   - right: elements greater than pivot
4. Recursively sort left and right
5. Concatenate: sorted(left) + middle + sorted(right)

Time complexity: O(n log n) average, O(n²) worst case
Space complexity: O(n) due to list comprehensions''',
                    "category": "code_understanding"
                },
                {
                    "instruction": "Explain this code",
                    "input": '''@lru_cache(maxsize=None)
def fibonacci(n):
    if n <= 1:
        return n
    return fibonacci(n-1) + fibonacci(n-2)''',
                    "output": '''This calculates Fibonacci numbers with memoization:
1. @lru_cache decorator caches function results
2. maxsize=None means unlimited cache
3. Recursive implementation:
   - Base cases: fib(0)=0, fib(1)=1
   - Recursive case: fib(n) = fib(n-1) + fib(n-2)
4. Cache prevents recalculation of same values

Without cache: O(2^n) time - extremely slow
With cache: O(n) time - each value calculated once
Trade-off: Uses O(n) memory for cache''',
                    "category": "code_understanding"
                }
            ])
        
        return examples
    
    def generate_test_generation(self) -> List[Dict]:
        """Generate test generation examples."""
        examples = []
        
        if self.language == "python":
            examples.extend([
                {
                    "instruction": "Write tests for this function",
                    "input": '''def is_palindrome(s: str) -> bool:
    s = ''.join(s.split()).lower()
    return s == s[::-1]''',
                    "output": '''import pytest

def test_is_palindrome_simple():
    """Test simple palindrome."""
    assert is_palindrome("racecar") == True
    assert is_palindrome("hello") == False

def test_is_palindrome_with_spaces():
    """Test palindrome with spaces."""
    assert is_palindrome("race car") == True
    assert is_palindrome("hello world") == False

def test_is_palindrome_mixed_case():
    """Test palindrome with mixed case."""
    assert is_palindrome("RaceCar") == True
    assert is_palindrome("Race") == False

def test_is_palindrome_empty():
    """Test empty string."""
    assert is_palindrome("") == True

def test_is_palindrome_single_char():
    """Test single character."""
    assert is_palindrome("a") == True''',
                    "category": "test_generation"
                }
            ])
        
        elif self.language == "go":
            examples.extend([
                {
                    "instruction": "Write tests for this function",
                    "input": '''func IsPalindrome(s string) bool {
    s = strings.ToLower(strings.ReplaceAll(s, " ", ""))
    runes := []rune(s)
    for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
        if runes[i] != runes[j] {
            return false
        }
    }
    return true
}''',
                    "output": '''func TestIsPalindrome(t *testing.T) {
    tests := []struct {
        name     string
        input    string
        expected bool
    }{
        {"simple palindrome", "racecar", true},
        {"not palindrome", "hello", false},
        {"with spaces", "race car", true},
        {"mixed case", "RaceCar", true},
        {"empty string", "", true},
        {"single char", "a", true},
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := IsPalindrome(tt.input)
            if result != tt.expected {
                t.Errorf("IsPalindrome(%q) = %v, want %v",
                    tt.input, result, tt.expected)
            }
        })
    }
}''',
                    "category": "test_generation"
                }
            ])
        
        return examples
    
    def generate_optimization(self) -> List[Dict]:
        """Generate code optimization examples."""
        examples = []
        
        if self.language == "python":
            examples.extend([
                {
                    "instruction": "Optimize this code",
                    "input": '''def find_duplicates(lst):
    duplicates = []
    for i in range(len(lst)):
        for j in range(i + 1, len(lst)):
            if lst[i] == lst[j] and lst[i] not in duplicates:
                duplicates.append(lst[i])
    return duplicates

# Time: O(n²), Space: O(n)''',
                    "output": '''def find_duplicates(lst):
    """Find duplicates in O(n) time."""
    seen = set()
    duplicates = set()
    
    for item in lst:
        if item in seen:
            duplicates.add(item)
        else:
            seen.add(item)
    
    return list(duplicates)

# Optimized: Time: O(n), Space: O(n)
# Uses hash sets for O(1) lookup instead of nested loops''',
                    "category": "optimization"
                }
            ])
        
        return examples
    
    def generate_refactoring(self) -> List[Dict]:
        """Generate refactoring examples."""
        examples = []
        
        if self.language == "python":
            examples.extend([
                {
                    "instruction": "Refactor this code",
                    "input": '''def process_user_data(data):
    if data is not None:
        if 'name' in data:
            if len(data['name']) > 0:
                if 'email' in data:
                    if '@' in data['email']:
                        return {'status': 'success', 'user': data}
    return {'status': 'error', 'message': 'Invalid data'}''',
                    "output": '''def process_user_data(data):
    """Process and validate user data."""
    # Early returns for validation
    if not data:
        return {'status': 'error', 'message': 'No data provided'}
    
    if 'name' not in data or not data['name']:
        return {'status': 'error', 'message': 'Name required'}
    
    if 'email' not in data or '@' not in data['email']:
        return {'status': 'error', 'message': 'Valid email required'}
    
    return {'status': 'success', 'user': data}

# Improvements:
# - Early returns reduce nesting
# - Clear error messages
# - More readable logic flow''',
                    "category": "refactoring"
                }
            ])
        
        return examples
    
    def _deduplicate(self, examples: List[Dict]) -> List[Dict]:
        """Remove duplicate examples based on output content."""
        import hashlib
        unique = []
        
        for ex in examples:
            output_hash = hashlib.md5(ex['output'].encode()).hexdigest()
            if output_hash not in self.seen_outputs:
                self.seen_outputs.add(output_hash)
                unique.append(ex)
        
        return unique
    
    def generate_dataset(self, num_examples: int = 1000) -> List[Dict]:
        """Generate complete coding training dataset with deduplication."""
        print(f"🎯 Target: {num_examples} unique {self.language} coding examples\n")
        
        all_examples = []
        
        # Distribution: 40% generation, 30% bug fixing, 20% understanding, 10% testing
        generators = [
            (self.generate_code_generation, 0.40, "code generation"),
            (self.generate_bug_fixing, 0.30, "bug fixing"),
            (self.generate_code_understanding, 0.20, "code understanding"),
            (self.generate_test_generation, 0.10, "test generation"),
        ]
        
        for generator, proportion, name in generators:
            target = int(num_examples * proportion)
            print(f"  Generating {name}...")
            
            examples = []
            attempts = 0
            # Generate until we hit target or max attempts
            while len(examples) < target and attempts < 10:
                attempts += 1
                batch = self._deduplicate(generator())
                examples.extend(batch)
            
            final_batch = examples[:target]
            all_examples.extend(final_batch)
            print(f"    ✓ {len(final_batch)} unique examples (target: {target})")
        
        # Add optimization and refactoring examples
        print("  Generating optimization examples...")
        opt = self._deduplicate(self.generate_optimization())
        all_examples.extend(opt)
        print(f"    ✓ {len(opt)} unique examples")
        
        print("  Generating refactoring examples...")
        refactor = self._deduplicate(self.generate_refactoring())
        all_examples.extend(refactor)
        print(f"    ✓ {len(refactor)} unique examples")
        
        # Shuffle
        random.shuffle(all_examples)
        final = all_examples[:num_examples]
        
        print(f"\n✅ Generated {len(final)} unique examples")
        print(f"   Deduplication: {len(self.seen_outputs)} unique outputs")
        
        return final


def main():
    parser = argparse.ArgumentParser(
        description="Generate coding assistant training dataset"
    )
    parser.add_argument(
        "--language",
        type=str,
        default="python",
        choices=["python", "go", "javascript", "java"],
        help="Programming language"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/coding_assistant.jsonl",
        help="Output file path"
    )
    parser.add_argument(
        "--num-examples",
        type=int,
        default=1000,
        help="Number of examples to generate"
    )
    
    args = parser.parse_args()
    
    generator = CodingDatasetGenerator(args.language)
    examples = generator.generate_dataset(args.num_examples)
    
    # Save to file
    import os
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    
    with open(args.output, 'w') as f:
        for example in examples:
            f.write(json.dumps(example) + '\n')
    
    print(f"✓ Generated {len(examples)} examples for {args.language}")
    print(f"✓ Saved to {args.output}")
    
    # Statistics
    by_category = defaultdict(int)
    for ex in examples:
        by_category[ex.get('category', 'unknown')] += 1
    
    print("\nDataset Statistics:")
    for category, count in sorted(by_category.items()):
        print(f"  {category}: {count}")
    
    print("\nNext steps:")
    print(f"  1. Validate: python scripts/validate_dataset.py {args.output}")
    print(f"  2. Train: python train.py --dataset {args.output} --output_dir models/coding-assistant")
    print(f"  3. Export: python export_to_gguf.py --model_dir models/coding-assistant --output coding-assistant.gguf")


if __name__ == "__main__":
    main()
