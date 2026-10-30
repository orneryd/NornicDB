#!/usr/bin/env python3
"""
Validate training dataset quality.
Checks format, syntax, balance, and other quality metrics.
"""

import json
import argparse
from typing import List, Dict, Set
from collections import Counter
import sys


def check_format(examples: List[Dict]) -> List[str]:
    """Check if examples have required fields."""
    issues = []
    
    required_fields = ["instruction", "output"]
    for i, ex in enumerate(examples, 1):
        missing = [f for f in required_fields if f not in ex]
        if missing:
            issues.append(f"Line {i}: Missing required fields: {', '.join(missing)}")
        
        # Check for empty values
        for field in ["instruction", "output"]:
            if field in ex and not ex[field].strip():
                issues.append(f"Line {i}: Empty {field}")
    
    return issues


def check_duplicates(examples: List[Dict]) -> List[str]:
    """Check for duplicate outputs."""
    issues = []
    
    outputs = [ex["output"] for ex in examples]
    output_counts = Counter(outputs)
    
    duplicates = {out: count for out, count in output_counts.items() if count > 1}
    if duplicates:
        total_dups = sum(count - 1 for count in duplicates.values())
        issues.append(f"Found {total_dups} duplicate outputs ({len(duplicates)} unique)")
        
        if total_dups > len(examples) * 0.1:
            issues.append("⚠️  HIGH: More than 10% duplicates")
    
    return issues


def check_length_distribution(examples: List[Dict]) -> List[str]:
    """Check length statistics."""
    issues = []
    
    input_lengths = [len(ex.get("input", "")) for ex in examples]
    output_lengths = [len(ex["output"]) for ex in examples]
    
    avg_input = sum(input_lengths) / len(input_lengths) if input_lengths else 0
    avg_output = sum(output_lengths) / len(output_lengths)
    
    if avg_output < 10:
        issues.append(f"⚠️  Average output length very short: {avg_output:.1f} chars")
    
    if avg_output > 1000:
        issues.append(f"⚠️  Average output length very long: {avg_output:.1f} chars")
    
    # Check for outliers
    max_output = max(output_lengths)
    if max_output > avg_output * 10:
        issues.append(f"⚠️  Some outputs much longer than average (max: {max_output})")
    
    return issues


def check_balance(examples: List[Dict]) -> List[str]:
    """Check dataset balance."""
    issues = []
    
    # Check if examples have categories
    if "category" in examples[0]:
        categories = Counter(ex["category"] for ex in examples)
        max_count = max(categories.values())
        min_count = min(categories.values())
        
        if max_count / min_count > 3:
            issues.append(f"⚠️  Dataset imbalanced (ratio {max_count/min_count:.1f}:1)")
            issues.append(f"   Category distribution: {dict(categories)}")
    
    # For Cypher: check query type distribution
    if any("MATCH" in ex["output"] for ex in examples[:10]):
        query_types = Counter()
        for ex in examples:
            output = ex["output"]
            if "MATCH" in output and "CREATE" not in output:
                query_types["MATCH"] += 1
            elif "CREATE" in output:
                query_types["CREATE"] += 1
            elif "MERGE" in output:
                query_types["MERGE"] += 1
            elif "DELETE" in output:
                query_types["DELETE"] += 1
        
        if query_types:
            total = sum(query_types.values())
            issues.append(f"ℹ️  Query type distribution:")
            for qtype, count in query_types.most_common():
                pct = 100 * count / total
                issues.append(f"   {qtype}: {count} ({pct:.1f}%)")
    
    return issues


def validate_cypher_syntax(examples: List[Dict]) -> List[str]:
    """Basic Cypher syntax validation."""
    issues = []
    
    # Simple syntax checks
    for i, ex in enumerate(examples, 1):
        output = ex["output"]
        
        # Check for common syntax errors
        if output.count("(") != output.count(")"):
            issues.append(f"Line {i}: Unbalanced parentheses")
        
        if output.count("[") != output.count("]"):
            issues.append(f"Line {i}: Unbalanced brackets")
        
        if output.count("{") != output.count("}"):
            issues.append(f"Line {i}: Unbalanced braces")
        
        # Check for valid Cypher keywords
        cypher_keywords = ["MATCH", "CREATE", "MERGE", "DELETE", "SET", "REMOVE", 
                          "RETURN", "WHERE", "WITH", "UNWIND", "ORDER BY", "LIMIT"]
        has_keyword = any(kw in output.upper() for kw in cypher_keywords)
        if not has_keyword and len(output) > 10:
            issues.append(f"Line {i}: No Cypher keywords found")
    
    return issues


def generate_report(examples: List[Dict], all_issues: Dict[str, List[str]]) -> str:
    """Generate comprehensive validation report."""
    report = []
    report.append("="*60)
    report.append("Dataset Validation Report")
    report.append("="*60)
    report.append(f"\nTotal Examples: {len(examples)}")
    
    # Statistics
    input_lengths = [len(ex.get("input", "")) for ex in examples]
    output_lengths = [len(ex["output"]) for ex in examples]
    
    report.append(f"\nLength Statistics:")
    report.append(f"  Avg Input:  {sum(input_lengths)/len(input_lengths):.1f} chars")
    report.append(f"  Avg Output: {sum(output_lengths)/len(output_lengths):.1f} chars")
    report.append(f"  Max Output: {max(output_lengths)} chars")
    report.append(f"  Min Output: {min(output_lengths)} chars")
    
    # Issues by category
    total_issues = sum(len(issues) for issues in all_issues.values())
    
    report.append(f"\n{'='*60}")
    if total_issues == 0:
        report.append("✅ No issues found! Dataset looks good.")
    else:
        report.append(f"⚠️  Found {total_issues} potential issues:")
        
        for category, issues in all_issues.items():
            if issues:
                report.append(f"\n{category}:")
                for issue in issues:
                    report.append(f"  {issue}")
    
    report.append("="*60)
    
    return "\n".join(report)


def main():
    parser = argparse.ArgumentParser(description="Validate training dataset")
    parser.add_argument("dataset", type=str, help="Path to dataset JSONL file")
    parser.add_argument("--check-cypher", action="store_true",
                      help="Perform Cypher syntax validation")
    
    args = parser.parse_args()
    
    # Load dataset
    try:
        with open(args.dataset, 'r') as f:
            examples = [json.loads(line) for line in f]
        print(f"Loaded {len(examples)} examples from {args.dataset}")
    except Exception as e:
        print(f"Error loading dataset: {e}")
        sys.exit(1)
    
    # Run validations
    all_issues = {}
    
    all_issues["Format"] = check_format(examples)
    all_issues["Duplicates"] = check_duplicates(examples)
    all_issues["Length Distribution"] = check_length_distribution(examples)
    all_issues["Balance"] = check_balance(examples)
    
    if args.check_cypher:
        all_issues["Cypher Syntax"] = validate_cypher_syntax(examples)
    
    # Generate and print report
    report = generate_report(examples, all_issues)
    print("\n" + report)
    
    # Exit code
    total_critical = sum(
        1 for issues in all_issues.values() 
        for issue in issues 
        if issue.startswith("Line")
    )
    
    if total_critical > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
