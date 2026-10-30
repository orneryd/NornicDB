# Example Training Datasets

This directory contains example datasets for training custom models.

## Available Examples

### 1. TLP Evaluation (`tlp_evaluation.jsonl`)

Train a model to evaluate TLP (Traffic Light Protocol) levels for data classification.

**Format**: Instruction-following

**Use case**: Data sensitivity classification, security labeling

**Size**: 5 examples (expand with your own data)

**Training command**:
```bash
python train.py --dataset data/examples/tlp_evaluation.jsonl --output_dir models/tlp-evaluator
```

### 2. TLP Chat Format (`tlp_chat.jsonl`)

Same TLP evaluation but in chat/conversation format.

**Format**: Chat (messages array)

**Use case**: Conversational TLP classification

**Training command**:
```bash
python train.py --dataset data/examples/tlp_chat.jsonl --output_dir models/tlp-chat
```

## Creating Your Own Dataset

### Instruction-Following Format

Best for: Task-specific models, clear input/output patterns

```jsonl
{"instruction": "What to do", "input": "The data", "output": "Expected response"}
{"instruction": "Another task", "input": "More data", "output": "Another response"}
```

### Chat Format

Best for: Conversational models, multi-turn interactions

```jsonl
{"messages": [{"role": "system", "content": "System prompt"}, {"role": "user", "content": "User message"}, {"role": "assistant", "content": "Assistant response"}]}
```

### Completion Format

Best for: Code completion, text generation

```jsonl
{"text": "Full training text with prompt and response together"}
```

## Dataset Quality Tips

1. **Size**: Minimum 50-100 examples, optimal 500-5000+
2. **Diversity**: Cover different scenarios and edge cases
3. **Quality**: Clean, consistent, representative data
4. **Balance**: Even distribution across categories
5. **Format**: Consistent structure, valid JSON

## Example: Expanding TLP Dataset

To create a production-ready TLP classifier:

1. **Gather examples** (500+ recommended):
   - Public data (TLP:CLEAR)
   - Partner data (TLP:GREEN)
   - Internal data (TLP:AMBER)
   - Confidential data (TLP:RED)

2. **Create training file**:
```bash
# Generate from your data
python -c "
import json
examples = [
    {'instruction': 'Evaluate TLP', 'input': 'Public blog post', 'output': 'TLP:CLEAR'},
    {'instruction': 'Evaluate TLP', 'input': 'Customer emails', 'output': 'TLP:AMBER'},
    # ... more examples
]
with open('data/tlp_full.jsonl', 'w') as f:
    for ex in examples:
        f.write(json.dumps(ex) + '\n')
"
```

3. **Train**:
```bash
python train.py --dataset data/tlp_full.jsonl --epochs 5 --output_dir models/tlp-prod
```

4. **Evaluate**:
   - Manual testing with sample inputs
   - Cross-validation with held-out data
   - A/B testing in production

## Other Dataset Ideas

### Security Classification
```jsonl
{"instruction": "Classify security risk", "input": "Open SSH port 22 on public server", "output": "HIGH RISK"}
```

### Code Review
```jsonl
{"instruction": "Review this code", "input": "def func():\n  password = '123'", "output": "SECURITY ISSUE: Hardcoded credential"}
```

### Data Quality Assessment
```jsonl
{"instruction": "Assess data quality", "input": "CSV with 30% missing values", "output": "POOR - High missing rate"}
```

### Incident Triage
```jsonl
{"instruction": "Triage incident", "input": "Database down, users unable to login", "output": "CRITICAL - P0"}
```

## Dataset Validation

Before training, validate your dataset:

```python
python -c "
import json
valid = 0
with open('data/your_dataset.jsonl', 'r') as f:
    for i, line in enumerate(f, 1):
        try:
            obj = json.loads(line)
            # Check required fields
            if 'instruction' in obj and 'output' in obj:
                valid += 1
        except:
            print(f'Invalid JSON on line {i}')
print(f'Valid examples: {valid}')
"
```

## Resources

- [HuggingFace Datasets](https://huggingface.co/datasets)
- [Instruction Datasets Collection](https://github.com/databrickslabs/dolly)
- [OpenAI Fine-tuning Guide](https://platform.openai.com/docs/guides/fine-tuning)
