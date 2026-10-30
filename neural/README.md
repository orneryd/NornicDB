# Neural Training System

**Train custom LLMs for NornicDB** - Fine-tune models on specialized tasks (TLP evaluation, etc.) and export to GGUF format for native inference.

## Overview

This system enables you to:
- **Train/Fine-tune** transformer models on custom datasets
- **Optimize for 2080 Ti** (11GB VRAM) with memory-efficient techniques
- **Export to GGUF** format compatible with NornicDB's llama.cpp integration
- **Load and use** the trained model just like any other GGUF model

## Architecture

```
Training Pipeline:
┌─────────────────┐     ┌──────────────┐     ┌─────────────┐     ┌──────────────┐
│   Dataset       │────▶│   PyTorch    │────▶│   Trained   │────▶│  GGUF File   │
│   (JSONL)       │     │   Training   │     │   Model     │     │  (Loadable)  │
└─────────────────┘     └──────────────┘     └─────────────┘     └──────────────┘
                              │                      │
                              ├─ LoRA/QLoRA         ├─ Safetensors
                              ├─ Gradient Checkpt   └─ GGUF conversion
                              ├─ Mixed Precision
                              └─ 2080 Ti Optimized
```

## Quick Start

### 1. Install Dependencies

```bash
cd neural
pip install -r requirements.txt
```

### 2. Prepare Training Data

Create a JSONL file with your training examples:

```jsonl
{"instruction": "Evaluate the TLP level for this data", "input": "Customer email addresses", "output": "TLP:AMBER - Limited distribution"}
{"instruction": "Evaluate the TLP level for this data", "input": "Public API documentation", "output": "TLP:CLEAR - Public information"}
```

### 3. Train the Model

```bash
# Basic training (auto-detects hardware)
python train.py \
  --base_model "TinyLlama/TinyLlama-1.1B-Chat-v1.0" \
  --dataset data/tlp_training.jsonl \
  --output_dir models/tlp-evaluator \
  --batch_size 4 \
  --gradient_accumulation 8 \
  --epochs 3

# Cypher query expert (using synthetic dataset)
python scripts/generate_cypher_dataset.py --count 2000 --output data/cypher_full.jsonl
python train.py \
  --preset qwen_1_5b \
  --dataset data/cypher_full.jsonl \
  --output_dir models/cypher-expert \
  --epochs 5

# Apple Silicon (M1/M2/M3) - automatically uses Metal
python train.py \
  --preset qwen_1_5b \
  --dataset data/training.jsonl \
  --output_dir models/trained
# Note: Metal acceleration is auto-detected, no special flags needed
```

### 4. Export to GGUF

```bash
python export_to_gguf.py \
  --model_dir models/tlp-evaluator \
  --output tlp-evaluator-q4.gguf \
  --quantization Q4_K_M
```

### 5. Use in NornicDB

Place the GGUF file in your models directory and load it:

```go
// In your Go code
opts := localllm.DefaultGenerationOptions("/data/models/tlp-evaluator-q4.gguf")
model, err := localllm.LoadGenerationModel(opts)
if err != nil {
    log.Fatal(err)
}
defer model.Close()

response, err := model.Generate(ctx, "Evaluate TLP: Internal salary data", params)
// Output: "TLP:RED - Highly confidential..."
```

## Training Configuration

### Hardware Support

The training system automatically detects and uses the best available hardware:

#### NVIDIA GPU (CUDA)
- ✅ **2080 Ti** (11GB) - Primary target
- ✅ **30xx/40xx Series** (8GB+) - Excellent performance
- ✅ **Data Center GPUs** (A100, H100, etc.)

#### Apple Silicon (Metal)
- ✅ **M1/M2/M3** - Native Metal acceleration via PyTorch MPS backend
- ✅ **Unified Memory** - Efficient memory usage across CPU/GPU
- ✅ **Automatic Fallback** - Unsupported ops run on CPU

#### CPU Fallback
- ⚠️ Supported but slow - Recommended only for small models/testing

### Memory Optimization

The training script automatically applies:
- **LoRA/QLoRA**: Train only 1-2% of parameters
- **Gradient Checkpointing**: Trade compute for memory
- **Mixed Precision (FP16/BF16)**: 2x memory reduction
- **Batch Size Management**: Gradient accumulation for effective large batches
- **Flash Attention 2**: Faster + less memory (CUDA only)
- **Metal Acceleration**: Native PyTorch MPS support on Apple Silicon

### Recommended Base Models

| Model | Size | VRAM (Training) | Best For |
|-------|------|-----------------|----------|
| TinyLlama-1.1B | 1.1B params | ~6GB | Quick experiments, small tasks |
| qwen3-0.6b | 500M params | ~4GB | Ultra-efficient, specific tasks |
| Qwen2.5-1.5B | 1.5B params | ~8GB | Balanced quality/efficiency |
| Phi-3-mini | 3.8B params | ~10GB | High quality, fits 2080 Ti |

## Dataset Format

### Instruction-Following Format

```jsonl
{
  "instruction": "What to do",
  "input": "The data to process",
  "output": "Expected response"
}
```

### Chat Format

```jsonl
{
## Dataset Generation

The `neural/scripts/` directory contains specialized dataset generators for different domains:

### 1. Cypher Query Dataset

Generate Cypher training data with comprehensive syntax coverage:

```bash
python scripts/generate_cypher_dataset.py \
    --output data/cypher_queries.jsonl \
    --num-examples 2000
```

**Covers:**
- Basic pattern matching (30%)
- Property filtering with WHERE (20%)
- Relationship queries (15%)
- Aggregations (12%)
- Complex multi-clause queries (15%)
- Systematic syntax enumeration (18% - ALL constructs)

### 2. Repository Code Patterns

Extract training data from codebases:

```bash
python scripts/extract_repo_patterns.py \
    --repo /Users/timothysweet/src/NornicDB \
    --language go \
    --output data/nornicdb_patterns.jsonl
```

**Extracts:**
- Function implementations with comments
- Type definitions (structs, interfaces)
- Test patterns
- API usage patterns
- Error handling
- Documentation

**Supports:** Go, Python, JavaScript, Java

### 3. MTG Judge Training

Generate Magic: The Gathering judge training data:

```bash
python scripts/generate_mtg_dataset.py \
    --output data/mtg_judge.jsonl \
    --num-examples 1000
```

**Covers:**
- Comprehensive Rules lookup
- Stack interactions (LIFO)
- Tournament rulings
- Card interactions
- Keyword abilities
- Complex scenarios

### 4. Coding Assistant

Generate general coding training data:

```bash
python scripts/generate_coding_dataset.py \
    --language python \
    --output data/coding_assistant.jsonl \
    --num-examples 1000
```

**Distribution:**
- Code generation (40%)
- Bug fixing (30%)
- Code understanding (20%)
- Test generation (10%)

**Supports:** Python, Go, JavaScript, Java

### Validating Datasets

Before training, validate quality:

```bash
python scripts/validate_dataset.py data/your_dataset.jsonl
```

**Checks:**
- Format correctness
- Duplicates
- Length distribution
- Category balance
- Syntax validation (for Cypher)

## Cypher Query Training

### Quick Start

Train a model to generate Cypher queries from natural language:

```bash
# 1. Generate dataset
python scripts/generate_cypher_dataset.py --output data/cypher.jsonl --num-examples 2000

# 2. Validate
python scripts/validate_dataset.py data/cypher.jsonl

# 3. Train
python train.py \
  --preset qwen_1_5b \
  --dataset data/cypher.jsonl \
  --output_dir models/cypher-expert \
  --epochs 5

# 4. Export to GGUF
python export_to_gguf.py \
  --model_dir models/cypher-expert \
  --output cypher-expert-q4.gguf

# 5. Use in NornicDB
cp cypher-expert-q4.gguf /data/models/
```

### Creating Custom Training Data

See detailed guides:
- [`docs/PERFECT_TRAINING_DATA.md`](docs/PERFECT_TRAINING_DATA.md) - Quality guidelines
- [`docs/DOMAIN_TRAINING_GUIDES.md`](docs/DOMAIN_TRAINING_GUIDES.md) - Domain-specific strategies

## Advanced Usage

### Configuring Model Size

Train models with custom parameter counts:

```python
from training import TrainingConfig

# Small model (100M params)
config = TrainingConfig(
    hidden_size=768,
    num_hidden_layers=12,
    num_attention_heads=12,
    intermediate_size=3072,
    vocab_size=32000,
    train_from_scratch=True
)

# Medium model (500M params)
config = TrainingConfig(
    hidden_size=1024,
    num_hidden_layers=24,
    num_attention_heads=16,
    intermediate_size=4096,
    vocab_size=32000,
    train_from_scratch=True
)

# Note: Training from scratch requires MUCH more data (10M+ tokens)
# Fine-tuning existing models is recommended for most use cases
```

### Custom Hyperparametersent": "Classify: customer database"},
    {"role": "assistant", "content": "TLP:RED - Critical data"}
  ]
}
```

### Completion Format

```jsonl
{
  "text": "### Instruction: Evaluate TLP\n### Input: public docs\n### Output: TLP:CLEAR"
}
```

## Advanced Usage

### Custom Hyperparameters

```python
from training.trainer import TrainingConfig

config = TrainingConfig(
    learning_rate=2e-4,
    lora_r=16,              # LoRA rank
    lora_alpha=32,          # LoRA scaling
    target_modules=["q_proj", "v_proj", "k_proj", "o_proj"],
    warmup_steps=100,
    max_grad_norm=1.0,
## Examples

See `data/examples/` for:
- **TLP evaluation** dataset
- **Cypher query generation** (via `scripts/generate_cypher_dataset.py`)

### Generate Training Data

```bash
# Cypher queries (1000 examples)
python scripts/generate_cypher_dataset.py --count 1000 --output data/cypher_training.jsonl

# Validate quality
python scripts/validate_dataset.py data/cypher_training.jsonl --check-cypher

# Preview examples
head -5 data/cypher_training.jsonl | python -m json.tool
```

## Creating Perfect Training Data

**Essential reading**: [`docs/PERFECT_TRAINING_DATA.md`](docs/PERFECT_TRAINING_DATA.md)

This comprehensive guide covers:
- ✅ General principles and best practices
- ✅ Dataset size recommendations
- ✅ Task-specific guides (Cypher, TLP, code generation, etc.)
- ✅ Data collection methods (manual, synthetic, hybrid)
- ✅ Quality assurance techniques
- ✅ Common pitfalls and how to avoid them

### Quick Tips

1. **Quality > Quantity**: 100 perfect examples > 1000 mediocre ones
2. **Cover Edge Cases**: Include 10-20% unusual scenarios
3. **Validate Syntax**: Use validation scripts before training
4. **Balance Distribution**: Equal representation across categories
5. **Iterate**: Start small, train, evaluate, expand where model failsn.py \
  --base_model "meta-llama/Llama-3.2-1B" \
  --dataset data/training.jsonl \
  --batch_size 2
```

### Quantization Options

| Quantization | Size | Quality | Speed |
|--------------|------|---------|-------|
| Q4_K_M | 25% of FP16 | Good | Fast |
| Q5_K_M | 31% of FP16 | Better | Moderate |
| Q8_0 | 50% of FP16 | Best | Slower |
| F16 | 100% | Perfect | Slowest |

## File Structure

```
neural/
├── README.md                 # This file
├── requirements.txt          # Python dependencies
├── train.py                  # Main training script
├── export_to_gguf.py        # GGUF conversion
├── config/
│   ├── training_config.yaml # Training configurations
│   └── model_configs/       # Model-specific configs
├── training/
│   ├── __init__.py
│   ├── trainer.py           # Training loop
│   ├── dataset.py           # Dataset loading
│   ├── metrics.py           # Evaluation metrics
│   └── utils.py             # Utilities
├── export/
│   ├── __init__.py
│   ├── converter.py         # HuggingFace → GGUF
│   └── quantize.py          # Quantization utilities
├── data/
│   ├── README.md            # Dataset documentation
│   └── examples/            # Example datasets
└── models/                   # Output directory
    └── checkpoints/         # Training checkpoints
```

## Troubleshooting

### Out of Memory (OOM)

1. **Reduce batch size**: `--batch_size 1`
2. **Increase gradient accumulation**: `--gradient_accumulation 16`
3. **Use smaller model**: Switch to qwen3-0.6b
4. **Enable QLoRA**: `--use_qlora` (4-bit quantized training)

### Slow Training

1. **Enable Flash Attention**: Auto-detected if available
2. **Optimize LoRA rank**: `--lora_r 8` (lower = faster)
3. **Reduce context length**: `--max_length 512`

### Export Issues

1. **Install llama.cpp**: Required for GGUF conversion
2. **Check model format**: Must be compatible with llama.cpp
3. **Verify quantization**: Some formats need specific llama.cpp versions

## Examples

See `data/examples/` for:
- **TLP evaluation** dataset
- **Sentiment analysis** dataset
- **Code review** dataset
- **Question answering** dataset

## Integration with NornicDB

Once you have a GGUF model:

```bash
# Copy to NornicDB models directory
cp models/tlp-evaluator-q4.gguf /data/models/

# Use with Heimdall scheduler
NORNICDB_MODELS_DIR=/data/models nornicdb
```

The model will be available alongside other GGUF models and can be loaded using the existing `localllm` package.

## Performance Tips

1. **Use QLoRA for large models**: 4-bit training uses ~50% less memory
2. **Monitor GPU utilization**: Use `nvidia-smi` to check VRAM usage
3. **Experiment with LoRA rank**: Lower rank = faster but potentially lower quality
4. **Validate early**: Use small dataset to verify training works
5. **Save checkpoints**: Resume training if interrupted

## Citation

If you use this training system, consider citing:

```bibtex
@software{nornicdb_neural,
  title = {NornicDB Neural Training System},
  author = {NornicDB Contributors},
  year = {2024},
  url = {https://github.com/orneryd/NornicDB}
}
```

## License

MIT License - Same as NornicDB
