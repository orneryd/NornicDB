#!/usr/bin/env python3
"""
Auto-correct dataset quality issues.

Fixes:
- Removes duplicates
- Balances categories
- Validates format
- Ensures minimum quality standards
"""

import json
import argparse
from pathlib import Path
from typing import List, Dict, Set
from collections import defaultdict, Counter
import hashlib


class DatasetCorrector:
    """Automatically fix common dataset issues."""
    
    def __init__(self, min_category_size: int = 50, max_category_ratio: float = 3.0):
        self.min_category_size = min_category_size
        self.max_category_ratio = max_category_ratio
        self.stats = {
            'original_count': 0,
            'duplicates_removed': 0,
            'balanced_count': 0,
            'invalid_removed': 0,
            'final_count': 0
        }
    
    def load_dataset(self, input_file: str) -> List[Dict]:
        """Load JSONL dataset."""
        examples = []
        with open(input_file, 'r') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    example = json.loads(line.strip())
                    examples.append(example)
                except json.JSONDecodeError as e:
                    print(f"⚠️  Skipping invalid JSON at line {line_num}: {e}")
        
        self.stats['original_count'] = len(examples)
        return examples
    
    def remove_duplicates(self, examples: List[Dict]) -> List[Dict]:
        """Remove duplicate examples based on output content."""
        seen = set()
        unique = []
        
        for ex in examples:
            # Create hash of output + input for deduplication
            content = ex.get('output', '') + '|' + ex.get('input', '')
            content_hash = hashlib.md5(content.encode()).hexdigest()
            
            if content_hash not in seen:
                seen.add(content_hash)
                unique.append(ex)
            else:
                self.stats['duplicates_removed'] += 1
        
        print(f"✓ Removed {self.stats['duplicates_removed']} duplicates")
        return unique
    
    def validate_format(self, examples: List[Dict]) -> List[Dict]:
        """Remove examples with invalid format."""
        valid = []
        
        for ex in examples:
            if not isinstance(ex, dict):
                self.stats['invalid_removed'] += 1
                continue
            
            # Check required fields
            if 'instruction' not in ex or 'input' not in ex or 'output' not in ex:
                self.stats['invalid_removed'] += 1
                continue
            
            # Check field types
            if not all(isinstance(ex[k], str) for k in ['instruction', 'input', 'output']):
                self.stats['invalid_removed'] += 1
                continue
            
            # Check minimum lengths
            if len(ex['output']) < 10 or len(ex['input']) < 5:
                self.stats['invalid_removed'] += 1
                continue
            
            valid.append(ex)
        
        if self.stats['invalid_removed'] > 0:
            print(f"✓ Removed {self.stats['invalid_removed']} invalid examples")
        
        return valid
    
    def balance_categories(self, examples: List[Dict]) -> List[Dict]:
        """Balance category distribution."""
        # Group by category
        by_category = defaultdict(list)
        for ex in examples:
            category = ex.get('category', 'unknown')
            by_category[category].append(ex)
        
        # Calculate target size (median of category sizes)
        sizes = [len(items) for items in by_category.values()]
        target_size = sorted(sizes)[len(sizes) // 2]
        
        # Ensure minimum size
        target_size = max(target_size, self.min_category_size)
        
        print(f"\n📊 Balancing categories (target: {target_size} examples each):")
        
        balanced = []
        for category, items in sorted(by_category.items()):
            original_size = len(items)
            
            if original_size < target_size:
                # Duplicate items to reach target
                multiplier = (target_size // original_size) + 1
                expanded = (items * multiplier)[:target_size]
                balanced.extend(expanded)
                print(f"  {category}: {original_size} → {len(expanded)} (duplicated)")
            
            elif original_size > target_size * self.max_category_ratio:
                # Downsample if too large
                sampled = items[:target_size]
                balanced.extend(sampled)
                print(f"  {category}: {original_size} → {len(sampled)} (downsampled)")
            
            else:
                balanced.extend(items)
                print(f"  {category}: {original_size} (kept)")
        
        self.stats['balanced_count'] = len(balanced)
        return balanced
    
    def shuffle(self, examples: List[Dict]) -> List[Dict]:
        """Shuffle examples."""
        import random
        random.shuffle(examples)
        return examples
    
    def save_dataset(self, examples: List[Dict], output_file: str):
        """Save corrected dataset."""
        with open(output_file, 'w') as f:
            for ex in examples:
                f.write(json.dumps(ex) + '\n')
        
        self.stats['final_count'] = len(examples)
        print(f"\n✓ Saved {len(examples)} examples to {output_file}")
    
    def print_stats(self):
        """Print correction statistics."""
        print("\n" + "="*60)
        print("Correction Statistics")
        print("="*60)
        print(f"Original examples:     {self.stats['original_count']}")
        print(f"Duplicates removed:    {self.stats['duplicates_removed']}")
        print(f"Invalid removed:       {self.stats['invalid_removed']}")
        print(f"After balancing:       {self.stats['balanced_count']}")
        print(f"Final count:           {self.stats['final_count']}")
        print(f"Reduction:             {self.stats['original_count'] - self.stats['final_count']} ({(1 - self.stats['final_count']/self.stats['original_count'])*100:.1f}%)")
        print("="*60)
    
    def correct(self, input_file: str, output_file: str):
        """Run all corrections."""
        print(f"📝 Loading dataset from {input_file}...")
        examples = self.load_dataset(input_file)
        
        print(f"\n🔍 Validating format...")
        examples = self.validate_format(examples)
        
        print(f"\n🧹 Removing duplicates...")
        examples = self.remove_duplicates(examples)
        
        print(f"\n⚖️  Balancing categories...")
        examples = self.balance_categories(examples)
        
        print(f"\n🔀 Shuffling...")
        examples = self.shuffle(examples)
        
        print(f"\n💾 Saving corrected dataset...")
        self.save_dataset(examples, output_file)
        
        self.print_stats()


def main():
    parser = argparse.ArgumentParser(
        description="Auto-correct dataset quality issues (edits in-place with backup)"
    )
    parser.add_argument(
        "input",
        help="Input dataset file (JSONL) - will be edited in place"
    )
    parser.add_argument(
        "--min-category-size",
        type=int,
        default=50,
        help="Minimum examples per category (default: 50)"
    )
    parser.add_argument(
        "--max-category-ratio",
        type=float,
        default=3.0,
        help="Maximum ratio between largest and smallest category (default: 3.0)"
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Skip creating backup (not recommended)"
    )
    
    args = parser.parse_args()
    
    input_path = Path(args.input)
    
    # Always create backup unless explicitly disabled
    if not args.no_backup:
        import shutil
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = input_path.parent / f"{input_path.stem}_backup_{timestamp}{input_path.suffix}"
        shutil.copy2(args.input, backup_file)
        print(f"📦 Created backup: {backup_file}\n")
    
    # Run correction
    corrector = DatasetCorrector(
        min_category_size=args.min_category_size,
        max_category_ratio=args.max_category_ratio
    )
    corrector.correct(args.input, output_file)
    
    # Run correction (output to same file = in-place edit)
    corrector = DatasetCorrector(
        min_category_size=args.min_category_size,
        max_category_ratio=args.max_category_ratio
    )
    corrector.correct(args.input, args.input)  # Same file = in-place
    
    print("\n✅ Dataset correction complete!")
    print(f"   Original file updated: {args.input}")
    if not args.no_backup:
        print(f"   Backup saved: {backup_file}")
    
    print(f"\nNext steps:")
    print(f"  1. Validate: python scripts/validate_dataset.py {args.input}")
    print(f"  2. Train: python train.py --preset heimdall --dataset {args.input}")